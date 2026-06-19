#include "proxy.hpp"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdio>

#include "log.hpp"
#include "metrics.hpp"

namespace jdblance {

void Exchange::abandon() {
  if (endpoint) endpoint->add_inflight(-1);
  if (socket) socket->close();
}

void Proxy::CancelToken::cancel() {
  cancelled = true;
  if (auto s = socket.lock()) s->cancel();
}

// ---------------------------------------------------------------------------
// Cluster
// ---------------------------------------------------------------------------

namespace {
std::optional<double> burst_of(double burst) {
  if (burst > 0) return burst;
  return std::nullopt;
}

std::unique_ptr<KeyedRateLimiter> route_limiter(const RouteRateLimit& spec) {
  if (spec.rate <= 0) return nullptr;
  return std::make_unique<KeyedRateLimiter>(spec.rate, burst_of(spec.burst));
}

bool same_limit(const RouteRateLimit& a, const RouteRateLimit& b) {
  return a.rate == b.rate && a.burst == b.burst && a.key == b.key;
}
}  // namespace

Cluster::Cluster(const Config& cfg)
    : retry_budget(cfg.retry.budget_ratio, cfg.retry.budget_min_per_sec),
      rate_limiter(std::make_unique<KeyedRateLimiter>(cfg.limits.rate_per_ip, burst_of(cfg.limits.rate_burst))),
      admission(AdmissionController::from_config(cfg.limits.concurrency)),
      health(cfg.health) {
  apply_config(cfg);
}

void Cluster::apply_config(const Config& cfg) {
  retry_budget.configure(cfg.retry.budget_ratio, cfg.retry.budget_min_per_sec);
  // Rebuilding forgets every client's bucket, so only do it on a real change.
  if (rate_limiter->rate() != cfg.limits.rate_per_ip || burst_of(cfg.limits.rate_burst) != rate_burst_) {
    rate_limiter = std::make_unique<KeyedRateLimiter>(cfg.limits.rate_per_ip, burst_of(cfg.limits.rate_burst));
  }
  rate_burst_ = burst_of(cfg.limits.rate_burst);
  admission->configure(cfg.limits.concurrency);
  if (cfg.cache.enabled) {
    cache.configure(static_cast<size_t>(cfg.cache.max_bytes), static_cast<size_t>(cfg.cache.max_object_bytes));
  } else {
    cache.configure(0, 0);
  }

  // Upstreams: survivors (by name) keep their balancer and outlier state.
  std::vector<EndpointPtr> previous = endpoints_;
  std::vector<std::shared_ptr<Upstream>> rebuilt;
  for (const UpstreamConfig& spec : cfg.all_upstreams()) {
    std::shared_ptr<Upstream> up = upstream(spec.name);
    if (!up) up = std::make_shared<Upstream>(spec.name);
    up->apply(spec, cfg, previous);
    rebuilt.push_back(std::move(up));
  }
  upstreams_ = std::move(rebuilt);

  endpoints_.clear();
  for (const auto& up : upstreams_) {
    endpoints_.insert(endpoints_.end(), up->endpoints().begin(), up->endpoints().end());
  }
  for (const EndpointPtr& old : previous) {
    if (std::find(endpoints_.begin(), endpoints_.end(), old) != endpoints_.end()) continue;
    old->pool.clear();
    bool name_survives = std::any_of(endpoints_.begin(), endpoints_.end(),
                                     [&](const EndpointPtr& e) { return e->name == old->name; });
    if (!name_survives) metrics::forget_endpoint(old->name);
  }
  for (const EndpointPtr& e : endpoints_) {
    metrics::ENDPOINT_UP.set({e->name}, e->healthy ? 1 : 0);
    metrics::ENDPOINT_BREAKER.set({e->name}, static_cast<double>(e->breaker.state()));
    metrics::ENDPOINT_INFLIGHT.set({e->name}, e->inflight);
    // Rare-event counters must exist at 0 before their first event: a series
    // that first appears at 1 is invisible to increase(), so the first
    // ejection would never show on the dashboard or fire its alert.
    metrics::ENDPOINT_EJECTIONS.inc({e->name}, 0);
    metrics::OUTLIER_EJECTIONS.inc({e->name}, 0);
  }

  // Routes: a survivor with the same limit keeps its buckets.
  std::vector<RoutePtr> routes;
  for (const RouteConfig& spec : cfg.routes) {
    auto route = std::make_shared<Route>();
    route->spec = spec;
    route->upstream = upstream(spec.upstream);
    auto old = std::find_if(routes_.begin(), routes_.end(),
                            [&](const RoutePtr& r) { return r->spec.name == spec.name; });
    if (old != routes_.end() && same_limit((*old)->spec.rate_limit, spec.rate_limit)) {
      route->limiter = std::move((*old)->limiter);
    } else {
      route->limiter = route_limiter(spec.rate_limit);
    }
    routes.push_back(std::move(route));
  }
  routes_ = std::move(routes);
  default_route_.reset();
  if (std::shared_ptr<Upstream> fallback = upstream(kDefaultUpstream)) {
    default_route_ = std::make_shared<Route>();
    default_route_->spec.name = kDefaultUpstream;
    default_route_->upstream = std::move(fallback);
  }

  health_enabled_ = cfg.health.enabled;
  health.update(cfg.health, endpoints_);
  if (manage_health_) {
    if (health_enabled_ && !health.running()) health.start();
    if (!health_enabled_ && health.running()) health.stop();
  }
}

void Cluster::start_health() {
  manage_health_ = true;
  if (health_enabled_ && !health.running()) health.start();
}

RoutePtr Cluster::route(const http::Request& req) const {
  if (!routes_.empty()) {
    std::string host = request_host(req);
    std::string_view path = req.path();
    for (const RoutePtr& r : routes_) {
      if (route_host_matches(r->spec.host, host) && route_prefix_matches(r->spec.prefix, path)) return r;
    }
  }
  return default_route_;
}

std::shared_ptr<Upstream> Cluster::upstream(std::string_view name) const {
  for (const auto& up : upstreams_) {
    if (up->name() == name) return up;
  }
  return nullptr;
}

size_t Cluster::usable() const {
  size_t total = 0;
  for (const auto& up : upstreams_) total += up->usable();
  return total;
}

bool Cluster::ready() const {
  double now = monotonic();
  return std::all_of(upstreams_.begin(), upstreams_.end(), [&](const auto& up) {
    return up->usable(now) > 0 && !up->in_panic(now);
  });
}

EndpointPtr Cluster::find(std::string_view name) const {
  for (const EndpointPtr& e : endpoints_) {
    if (e->name == name) return e;
  }
  return nullptr;
}

Json Cluster::snapshot() {
  double now = monotonic();
  bool panic = false;
  Json upstreams = Json::array();
  for (const auto& up : upstreams_) {
    upstreams.push_back(up->snapshot(now));
    panic = panic || up->in_panic(now);
  }
  Json out = Json::object();
  out["policy"] = primary().balancer().name();
  out["panic_mode"] = panic;
  out["healthy"] = usable();
  out["total"] = endpoints_.size();
  out["admission"] = admission->snapshot();
  out["retry_budget"] = retry_budget.snapshot();
  Json endpoints = Json::array();
  for (const EndpointPtr& e : endpoints_) endpoints.push_back(e->snapshot());
  out["endpoints"] = std::move(endpoints);
  out["upstreams"] = std::move(upstreams);
  Json routes = Json::array();
  for (const RoutePtr& r : routes_) {
    Json route = Json::object();
    route["name"] = r->spec.name;
    route["host"] = r->spec.host;
    route["prefix"] = r->spec.prefix;
    route["upstream"] = r->spec.upstream;
    route["rate_limit"] = r->limiter ? Json(r->spec.rate_limit.rate) : Json();
    routes.push_back(std::move(route));
  }
  out["routes"] = std::move(routes);
  Json cache_stats = Json::object();
  cache_stats["entries"] = cache.size();
  cache_stats["bytes"] = cache.bytes();
  cache_stats["hits"] = metrics::CACHE.value({"hit"});
  cache_stats["misses"] = metrics::CACHE.value({"miss"});
  out["cache"] = std::move(cache_stats);
  return out;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

bool client_wants_keepalive(const http::Request& req) {
  if (req.version == "HTTP/1.0") return req.connection_has("keep-alive");
  return !req.connection_has("close");
}

namespace {
// Whether the origin will keep this connection open after the response.
// HTTP/1.0 closes unless it said keep-alive; HTTP/1.1 stays open unless it
// said close; a read-until-close body ends the connection either way.
bool upstream_keeps_alive(const http::Response& resp, const http::Framing& framing) {
  if (framing.kind == http::BodyKind::Eof || resp.connection_has("close")) return false;
  return resp.version != "HTTP/1.0" || resp.connection_has("keep-alive");
}
}  // namespace

std::string metric_method(std::string_view method) {
  // Methods are client-controlled; an unbounded label set is a cardinality bomb.
  static const char* known[] = {"GET", "HEAD", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "TRACE"};
  for (const char* k : known) {
    if (method == k) return k;
  }
  return "OTHER";
}

namespace {
std::string random_id() {
  static const char* hex = "0123456789abcdef";
  uint64_t v = rng()();
  std::string out(16, '0');
  for (int i = 15; i >= 0; --i) {
    out[static_cast<size_t>(i)] = hex[v & 0xF];
    v >>= 4;
  }
  return out;
}

// A client-supplied request ID is propagated only if it is boring: it goes
// into upstream headers, error bodies and logs.
std::string request_id_for(const http::Request& req) {
  const std::string* given = req.get("x-request-id");
  if (given != nullptr && !given->empty() && given->size() <= 128 &&
      std::all_of(given->begin(), given->end(), [](char c) {
        return std::isalnum(static_cast<unsigned char>(c)) || c == '-' || c == '_' || c == '.' || c == ':';
      })) {
    return *given;
  }
  return random_id();
}

struct TunnelState {
  Event done;
  int running = 2;
};

// One direction of an upgraded connection.  EOF is propagated as a half-close
// so the other direction can finish; an error or idle timeout tears down both.
Task<void> pump(BufReader* from, SocketPtr from_socket, SocketPtr to, std::string direction,
                std::shared_ptr<TunnelState> state, double idle) {
  std::unique_ptr<char[]> buf(new char[http::kRelayChunk]);
  bool clean = false;
  try {
    for (;;) {
      size_t n = co_await from->read_into(buf.get(), http::kRelayChunk, monotonic() + idle);
      if (n == 0) {
        clean = true;
        break;
      }
      metrics::TUNNEL_BYTES.inc({direction}, static_cast<double>(n));
      co_await to->write_all(std::string_view(buf.get(), n), monotonic() + idle);
    }
  } catch (const std::exception&) {
  }
  if (clean) {
    to->shutdown_write();
  } else {
    from_socket->cancel();
    to->cancel();
  }
  --state->running;
  state->done.set();
}
}  // namespace

// ---------------------------------------------------------------------------
// Proxy: connection loop
// ---------------------------------------------------------------------------

Proxy::Proxy(const Config& cfg, Cluster& cluster, int worker_id)
    : cfg_(cfg), cluster_(cluster), worker_id_(worker_id) {}

void Proxy::start_drain() {
  draining_ = true;
  for (ClientConn* c : idle_) {
    if (c->served > 0) c->socket->cancel();
  }
}

void Proxy::close_all() {
  for (ClientConn* c : conns_) c->socket->cancel();
}

Task<void> Proxy::handle_client(SocketPtr socket) {
  if (static_cast<int>(conns_.size()) >= cfg_.limits.max_connections) {
    metrics::SHED.inc({"max_connections"});
    try {
      co_await socket->write_all(http::simple_response(503, "connection limit reached\n"),
                                 monotonic() + 1.0);
    } catch (const std::exception&) {
    }
    socket->close();
    co_return;
  }

  ClientConn conn{socket, BufReader(socket), socket->peer_ip(), 0};
  conns_.insert(&conn);
  metrics::ACTIVE_CONNS.set(static_cast<double>(conns_.size()));
  metrics::CONNS_TOTAL.inc();

  for (;;) {
    // While draining, a connection gets at most the request already on its way.
    if (draining_ && conn.served > 0) break;
    double timeout = conn.served == 0 ? cfg_.timeouts.client_header : cfg_.timeouts.client_idle;
    bool idle = conn.reader.buffered() == 0;
    if (idle) idle_.insert(&conn);
    std::optional<http::Request> req;
    std::optional<http::ProtocolError> bad;
    try {
      req = co_await http::read_request(conn.reader, monotonic() + timeout);
    } catch (const http::ProtocolError& e) {
      bad = e;
    } catch (const std::exception&) {
      // Timeout, clean close, reset, or cancelled by drain: just hang up.
    }
    idle_.erase(&conn);
    if (bad) {
      try {
        co_await socket->write_all(http::simple_response(bad->status, bad->message + "\n"),
                                   monotonic() + 5.0);
      } catch (const std::exception&) {
      }
      break;
    }
    if (!req) break;
    ++conn.served;
    bool keep = false;
    try {
      keep = co_await serve(conn, *req);
    } catch (const std::exception& e) {
      log::error("unhandled error serving " + conn.peer_ip + ": " + e.what());
    }
    if (!keep) break;
  }

  idle_.erase(&conn);
  conns_.erase(&conn);
  metrics::ACTIVE_CONNS.set(static_cast<double>(conns_.size()));
  socket->close();
}

// ---------------------------------------------------------------------------
// Proxy: one request
// ---------------------------------------------------------------------------

Task<bool> Proxy::serve(ClientConn& conn, http::Request& req) {
  RequestCtx ctx;
  ctx.started = monotonic();
  ctx.request_id = request_id_for(req);
  ctx.peer_ip = conn.peer_ip;
  ctx.source_ip = effective_client_ip(req, conn.peer_ip);
  bool keepalive = client_wants_keepalive(req) && !draining_;
  bool head_only = req.method == "HEAD";

  http::Framing framing;
  int early_status = 0;
  std::string early_reason;
  http::Headers early_extra;
  try {
    framing = http::request_framing(req);
  } catch (const http::ProtocolError& e) {
    early_status = e.status;
    early_reason = e.message;
  }
  if (early_status == 0 && req.method == "CONNECT") {
    // A reverse proxy has no business opening arbitrary tunnels.
    early_status = 405;
    early_reason = "CONNECT is not supported";
  }
  // Rate limit before touching the body: a limited client should not get to
  // spend our bandwidth uploading first.
  if (early_status == 0 && !cluster_.rate_limiter->allow(ctx.source_ip)) {
    metrics::SHED.inc({"rate_limit"});
    early_status = 429;
    early_reason = "rate limit exceeded";
    early_extra = {{"Retry-After", "1"}};
  }
  RoutePtr route;
  if (early_status == 0) {
    route = cluster_.route(req);
    if (!route) {
      early_status = 404;
      early_reason = "no route for this request";
    }
  }
  if (early_status == 0) {
    ctx.route = route->spec.name;
    metrics::ROUTE_REQUESTS.inc({ctx.route});
    if (route->limiter) {
      std::string bucket = ctx.source_ip;
      const std::string& kind = route->spec.rate_limit.key;
      if (kind.rfind("header:", 0) == 0) {
        const std::string* value = req.get(std::string_view(kind).substr(7));
        // Prefixed so a header value can never share a bucket with an IP.
        if (value != nullptr && !value->empty()) bucket = "h:" + *value;
      }
      if (!route->limiter->allow(bucket)) {
        metrics::SHED.inc({"route_rate_limit"});
        early_status = 429;
        early_reason = "rate limit exceeded";
        early_extra = {{"Retry-After", "1"}};
      }
    }
  }
  if (early_status != 0) {
    ctx.status = early_status;
    co_await send_error(conn, early_status, early_reason, ctx.request_id, head_only, early_extra);
    finish(req, ctx);
    co_return false;
  }

  // A cache hit costs no admission slot and no backend.
  std::string cache_key;
  if (cfg_.cache.enabled && route->spec.cache && request_may_use_cache(req, framing)) {
    cache_key = route->spec.name + "\n" + request_host(req) + "\n" + req.target;
    if (std::shared_ptr<const CachedResponse> hit = cluster_.cache.get(cache_key, ctx.started)) {
      metrics::CACHE.inc({"hit"});
      ctx.cache = "hit";
      ctx.endpoint = "cache";
      ctx.status = hit->status;
      bool keep = co_await serve_cached(conn, req, *hit, keepalive);
      finish(req, ctx);
      co_return keep;
    }
    metrics::CACHE.inc({"miss"});
    ctx.cache = "miss";
    if (head_only) cache_key.clear();   // a HEAD response has no body to store
  }

  bool admitted = co_await cluster_.admission->acquire();
  if (!admitted) {
    ctx.status = 503;
    http::Headers retry_after = {{"Retry-After", "1"}};
    std::string reason = "overloaded";
    co_await send_error(conn, 503, reason, ctx.request_id, head_only, retry_after);
    finish(req, ctx);
    co_return false;
  }
  metrics::INFLIGHT.set(cluster_.admission->inflight());
  cluster_.retry_budget.record_request();
  // The limiter's clock starts *after* admission; see below.
  double admitted_at = monotonic();

  bool keep = false;
  try {
    keep = co_await forward(conn, req, framing, ctx, keepalive, route, std::move(cache_key));
  } catch (const std::exception& e) {
    log::error(std::string("unexpected error in forward: ") + e.what());
    if (ctx.status == 0) ctx.status = 500;
  }

  cluster_.admission->release();
  metrics::INFLIGHT.set(cluster_.admission->inflight());
  if (GradientLimiter* limiter = cluster_.admission->limiter(); limiter != nullptr && ctx.status) {
    // Feed the limiter the *service* time, measured from admission.  The
    // end-to-end time includes waiting in the limiter's own queue, which turns
    // the control loop into positive feedback: queueing inflates the sample,
    // the limit shrinks, which causes more queueing, until the limit sits at
    // its floor while the backends idle.  Only an upstream timeout counts as a
    // drop: counting our own 503s would let one shed cause the next.
    limiter->update(monotonic() - admitted_at, cluster_.admission->inflight(), ctx.status == 504);
  }
  finish(req, ctx);
  co_return keep;
}

Task<bool> Proxy::forward(ClientConn& conn, http::Request& req, http::Framing framing,
                          RequestCtx& ctx, bool client_keepalive, RoutePtr route, std::string cache_key) {
  bool head_only = req.method == "HEAD";
  int fail_status = 0;
  std::string fail_reason;
  bool client_gone = false;
  bool response_started = false;
  bool keep_alive = false;

  try {
    // 100-continue only now, after admission: the request is definitely going
    // to be attempted, so "go ahead" is honest.  Never to HTTP/1.0 clients.
    if (req.version == "HTTP/1.1" && !framing.empty() && req.expects_continue()) {
      co_await conn.socket->write_all("HTTP/1.1 100 Continue\r\n\r\n",
                                      monotonic() + cfg_.timeouts.client_header);
    }

    // Buffer bodies of known length so the request stays retryable.  Chunked
    // uploads stream: their size is unknown until the terminator.
    std::shared_ptr<const std::string> body;
    if (framing.empty()) {
      body = std::make_shared<const std::string>();
    } else if (framing.kind == http::BodyKind::Length) {
      if (static_cast<double>(framing.length) > cfg_.limits.max_body_bytes) {
        throw http::ProtocolError("request body too large", 413);
      }
      std::optional<std::string> buffered = co_await http::read_body_into_memory(
          conn.reader, framing, static_cast<size_t>(cfg_.retry.buffer_bytes),
          monotonic() + cfg_.timeouts.client_idle);
      if (buffered) {
        metrics::BYTES_IN.inc(static_cast<double>(buffered->size()));
        body = std::make_shared<const std::string>(std::move(*buffered));
      }
    }

    std::shared_ptr<Upstream> upstream = route->upstream;
    // Copies: a reload may change the upstream's spec while this request waits.
    HashKeyConfig hash_key = upstream->spec().hash_key;
    StickyConfig sticky = upstream->spec().sticky;
    std::optional<std::string> key = affinity_key(req, ctx.source_ip, hash_key);
    std::string target =
        route->spec.strip_prefix ? strip_route_prefix(req.target, route->spec.prefix) : req.target;
    auto head = std::make_shared<const std::string>(build_upstream_head(req, framing, body.get(), ctx, target));
    std::string preferred;
    if (!sticky.cookie.empty()) preferred = cookie_value(req, sticky.cookie).value_or("");
    Dispatched d = co_await dispatch(conn, req, framing, body, key, head, upstream, preferred);
    Exchange& ex = d.exchange;
    ctx.endpoint = ex.endpoint->name;
    ctx.status = ex.response.status;
    ctx.attempts = d.attempts;
    ctx.hedged = d.hedged;

    ResponseExtras extras;
    extras.cache_key = std::move(cache_key);
    if (!sticky.cookie.empty() && ex.endpoint->sticky_token() != preferred) {
      // New client, or its backend went away: point it at the one that served it.
      extras.set_cookie = sticky.cookie + "=" + ex.endpoint->sticky_token() + "; Path=/; HttpOnly; SameSite=Lax";
      if (sticky.max_age > 0) {
        extras.set_cookie += "; Max-Age=" + std::to_string(static_cast<long long>(sticky.max_age));
      }
    }

    // From the first byte written, the request is no longer retryable.
    response_started = true;
    RelayOutcome out =
        co_await relay_response(conn, req, ex, client_keepalive, d.attempts, d.hedged, extras);
    ex.endpoint->add_inflight(-1);
    ex.endpoint->record(ex.response.status < 500, ex.response.status);
    ex.endpoint->observe_latency(ex.elapsed);
    bool reusable = out.upstream_reusable && ex.reader->buffered() == 0;
    ex.endpoint->release(std::move(ex.socket), reusable);
    metrics::UPSTREAM_DURATION.observe({ex.endpoint->name}, ex.elapsed);
    metrics::UPSTREAM_DURATION_ALL.observe(ex.elapsed);
    if (out.client_gone) client_gone = true;
    keep_alive = out.keep_alive;
  } catch (const UpstreamError& e) {
    fail_status = e.status;
    fail_reason = e.reason;
  } catch (const ClientError& e) {
    if (e.status) {
      fail_status = e.status;
      fail_reason = e.what();
    } else {
      client_gone = true;
    }
  } catch (const http::ProtocolError& e) {
    fail_status = e.status;
    fail_reason = e.message;
  } catch (const TimeoutError&) {
    // Only client-side waits surface as a bare timeout here; upstream ones
    // arrive as UpstreamError(504).
    fail_status = 408;
    fail_reason = "timed out reading the request";
  } catch (const http::SinkError&) {
    client_gone = true;
  } catch (const IoError&) {
    client_gone = true;
  } catch (const ConnectionClosed&) {
    client_gone = true;
  } catch (const CancelledError&) {
    client_gone = true;
  }

  if (client_gone) {
    ctx.status = 499;   // nginx's convention: the client went away
    co_return false;
  }
  if (fail_status != 0) {
    ctx.status = fail_status;
    http::Headers none;
    if (!response_started) {
      co_await send_error(conn, fail_status, fail_reason, ctx.request_id, head_only, none);
    }
    co_return false;
  }
  co_return keep_alive;
}

// ---------------------------------------------------------------------------
// Proxy: attempts
// ---------------------------------------------------------------------------

Task<Proxy::Dispatched> Proxy::dispatch(ClientConn& conn, const http::Request& req,
                                        http::Framing framing,
                                        std::shared_ptr<const std::string> body,
                                        const std::optional<std::string>& key,
                                        std::shared_ptr<const std::string> head,
                                        std::shared_ptr<Upstream> upstream, std::string preferred) {
  bool retryable = body != nullptr && cfg_.retry.enabled;
  int max_attempts = retryable ? cfg_.retry.max_attempts : 1;
  bool hedge_ok = cfg_.hedge.enabled && req.idempotent() && framing.empty() &&
                  upstream->endpoints().size() > 1;

  AttemptInput input;
  input.method = req.method;
  input.idempotent = req.idempotent();
  input.upgrade = req.upgrade_target().has_value();
  input.head = std::move(head);
  input.body = body;
  input.framing = framing;
  input.client = body ? nullptr : &conn.reader;

  std::vector<std::string> tried;
  std::optional<UpstreamError> last_error;
  for (int attempt = 0; attempt < max_attempts; ++attempt) {
    if (attempt > 0) {
      if (!cluster_.retry_budget.try_retry()) break;
      metrics::RETRIES.inc({last_error ? last_error->reason : "unknown"});
      // Exponential backoff with full jitter: deterministic backoff
      // re-synchronises every retrying client onto the same instant.
      double cap = std::min(cfg_.retry.backoff_max,
                            cfg_.retry.backoff_base * std::pow(2.0, attempt - 1));
      if (cap > 0) co_await sleep_for(uniform(0.0, cap));
    }
    // The sticky preference applies to the first attempt only: a retry is
    // exactly the case where that backend just failed.
    EndpointPtr endpoint = upstream->select(key, tried, attempt == 0 ? preferred : std::string());
    if (!endpoint) {
      if (last_error) break;
      throw UpstreamError("no healthy upstream", false, 503);
    }
    tried.push_back(endpoint->name);

    std::optional<Exchange> exchange;
    std::optional<UpstreamError> failure;
    bool hedged = false;
    try {
      if (hedge_ok && attempt == 0) {
        std::pair<Exchange, bool> raced = co_await hedged_attempt(endpoint, input, key, tried, *upstream);
        exchange.emplace(std::move(raced.first));
        hedged = raced.second;
      } else {
        Exchange single = co_await this->attempt(endpoint, input, nullptr);
        exchange.emplace(std::move(single));
      }
    } catch (const UpstreamError& e) {
      failure = e;
    }

    if (exchange) {
      int status = exchange->response.status;
      if (cfg_.retry.retry_on_5xx && status >= 500 && attempt + 1 < max_attempts && req.idempotent()) {
        last_error = UpstreamError("status_" + std::to_string(status), true, status);
        co_await discard(std::move(*exchange));
        continue;
      }
      co_return Dispatched{std::move(*exchange), attempt + 1, hedged};
    }
    last_error = failure;
    if (!failure->retryable || !retryable) throw *failure;
  }
  if (last_error) throw *last_error;
  throw UpstreamError("all attempts failed", false, 502);
}

struct Proxy::HedgeRace {
  Event wake;
  std::optional<Exchange> winner;
  int winner_index = -1;
  int pending = 0;
  bool decided = false;
  std::optional<UpstreamError> failure;
  std::shared_ptr<CancelToken> tokens[2];
};

Task<void> Proxy::race_runner(Proxy* self, std::shared_ptr<HedgeRace> race, int index,
                              EndpointPtr endpoint, AttemptInput input) {
  std::optional<Exchange> result;
  std::optional<UpstreamError> error;
  try {
    Exchange ex = co_await self->attempt(endpoint, input, race->tokens[index]);
    result.emplace(std::move(ex));
  } catch (const UpstreamError& e) {
    error = e;
  } catch (const CancelledError&) {
    // Lost the race and was cancelled; attempt() already cleaned up.
  } catch (const std::exception& e) {
    error = UpstreamError(e.what(), false);
  }
  --race->pending;
  if (result) {
    if (!race->winner && !race->decided) {
      race->winner.emplace(std::move(*result));
      race->winner_index = index;
    } else {
      result->abandon();
    }
  } else if (error && !race->decided) {
    race->failure = std::move(error);
  }
  race->wake.set();
}

Task<std::pair<Exchange, bool>> Proxy::hedged_attempt(EndpointPtr primary, AttemptInput input,
                                                      const std::optional<std::string>& key,
                                                      std::vector<std::string>& tried, Upstream& upstream) {
  std::optional<double> delay = hedge_delay();
  if (!delay) {
    Exchange ex = co_await attempt(primary, input, nullptr);
    co_return std::pair<Exchange, bool>(std::move(ex), false);
  }

  auto race = std::make_shared<HedgeRace>();
  race->tokens[0] = std::make_shared<CancelToken>();
  race->pending = 1;
  spawn(race_runner(this, race, 0, primary, input));

  bool launched = false;
  bool settled = co_await race->wake.wait(monotonic() + *delay);
  if (!settled && cluster_.retry_budget.try_retry()) {
    // Chosen only now that the hedge actually fires.
    EndpointPtr backup = upstream.select(key, tried);
    if (backup && backup != primary) {
      metrics::HEDGES.inc();
      tried.push_back(backup->name);
      race->tokens[1] = std::make_shared<CancelToken>();
      ++race->pending;
      launched = true;
      spawn(race_runner(this, race, 1, backup, input));
    }
  }

  for (;;) {
    if (race->winner) {
      race->decided = true;
      for (int i = 0; i < 2; ++i) {
        if (i != race->winner_index && race->tokens[i]) race->tokens[i]->cancel();
      }
      if (race->winner_index == 1) metrics::HEDGE_WINS.inc();
      Exchange ex = std::move(*race->winner);
      race->winner.reset();
      co_return std::pair<Exchange, bool>(std::move(ex), launched);
    }
    if (race->pending == 0) {
      race->decided = true;
      if (race->failure) throw *race->failure;
      throw UpstreamError("hedged attempts failed", true);
    }
    race->wake.clear();
    co_await race->wake.wait();
  }
}

Task<Exchange> Proxy::attempt(EndpointPtr endpoint, AttemptInput input,
                              std::shared_ptr<CancelToken> token) {
  enum Phase { kConnect, kSend, kResponse };
  double started = monotonic();
  endpoint->add_inflight(1);
  Phase phase = kConnect;
  SocketPtr sock;
  std::optional<Exchange> result;
  std::optional<ClientError> client_error;
  std::string error;
  bool timeout = false;
  bool cancelled = false;
  bool local = false;

  try {
    Endpoint::Conn conn = co_await endpoint->connect(cfg_.timeouts.connect);
    sock = std::move(conn.socket);
    if (token) {
      token->socket = sock;
      if (token->cancelled) throw CancelledError();
    }

    phase = kSend;
    double write_deadline = monotonic() + cfg_.timeouts.connect + 5.0;
    if (input.body && !input.body->empty() && input.body->size() <= 16 * 1024) {
      std::string wire = *input.head + *input.body;   // one segment for small bodies
      co_await sock->write_all(wire, write_deadline);
    } else {
      co_await sock->write_all(*input.head, write_deadline);
      if (input.body && !input.body->empty()) co_await sock->write_all(*input.body, write_deadline);
    }
    if (!input.body && !input.framing.empty()) {
      // Streaming body: this attempt is unrepeatable.  Failures reading the
      // client are the client's; failures writing upstream are the origin's.
      try {
        co_await http::relay_body(*input.client, http::Sink{sock.get(), nullptr}, input.framing,
                                  static_cast<uint64_t>(cfg_.limits.max_body_bytes),
                                  monotonic() + cfg_.timeouts.upstream_body);
      } catch (const http::SinkError&) {
        throw;
      } catch (const http::ProtocolError& e) {
        throw ClientError(e.message, e.status);
      } catch (const TimeoutError&) {
        throw ClientError("timed out reading the request body", 408);
      } catch (const IoError& e) {
        throw ClientError(e.what(), 0);
      }
    }

    phase = kResponse;
    auto reader = std::make_unique<BufReader>(sock);
    double header_deadline = monotonic() + cfg_.timeouts.upstream_header;
    http::Response response = co_await http::read_response(*reader, header_deadline);
    while (response.status >= 100 && response.status < 200 && response.status != 101) {
      // Interim 1xx (an origin answering a forwarded Expect): swallow.
      response = co_await http::read_response(*reader, header_deadline);
    }
    if (response.status == 101 && !input.upgrade) {
      throw http::ProtocolError("101 without an upgrade request", 502);
    }
    Exchange ex;
    ex.framing = http::response_framing(input.method, response);
    ex.endpoint = endpoint;
    ex.socket = sock;
    ex.reader = std::move(reader);
    ex.response = std::move(response);
    ex.reused = conn.reused;
    ex.started = started;
    ex.elapsed = monotonic() - started;
    result.emplace(std::move(ex));
  } catch (const CancelledError&) {
    cancelled = true;
  } catch (const ClientError& e) {
    client_error = e;
  } catch (const TimeoutError&) {
    timeout = true;
    error = phase == kConnect ? "connect: timeout" : "timeout";
  } catch (const ConnectionClosed&) {
    error = "closed before responding";
  } catch (const http::ProtocolError& e) {
    error = "protocol: " + e.message;
  } catch (const http::SinkError& e) {
    timeout = e.timeout;
    error = std::string("write: ") + e.what();
  } catch (const IoError& e) {
    error = std::string(phase == kConnect ? "connect: " : "") + e.what();
    local = is_local_resource_error(e.code());
  }

  if (result) co_return std::move(*result);
  endpoint->add_inflight(-1);
  if (sock) sock->close();
  if (cancelled) {
    // Lost a hedge race while waiting for its response.  Without this the
    // slow endpoint is never measured as slow: its EWMA decays to zero, P2C
    // keeps choosing it, every such request gets hedged, and the hedges
    // drain the retry budget that real failures need.
    if (phase == kResponse) endpoint->observe_lower_bound(monotonic() - started);
    throw CancelledError();
  }
  if (client_error) throw *client_error;
  if (local) {
    // Out of descriptors / ports / memory here: the backend did nothing
    // wrong, so do not trip its breaker, and another endpoint would fail
    // the same way, so do not retry.  We are the overloaded party.
    metrics::SHED.inc({"local_resources"});
    static double last_warned = 0;
    if (monotonic() - last_warned > 1.0) {   // at most once a second: this repeats under load
      last_warned = monotonic();
      log::warning("upstream " + endpoint->name + ": " + error);
    }
    throw UpstreamError("proxy out of resources", false, 503);
  }
  endpoint->record(false, 0, error);
  // An origin cannot act on a request it never fully received: until the
  // request is written, any method may be retried.  After that, only an
  // idempotent one -- replaying a POST the origin may have processed is how
  // a proxy double-charges a card.
  bool retryable = phase != kResponse || input.idempotent;
  std::string reason = phase == kConnect ? "connect" : timeout ? "timeout" : "upstream";
  throw UpstreamError(reason, retryable, timeout ? 504 : 502);
}

Task<void> Proxy::discard(Exchange ex) {
  ex.endpoint->add_inflight(-1);
  ex.endpoint->record(false, ex.response.status);
  bool reusable = false;
  try {
    co_await http::relay_body(*ex.reader, http::Sink{}, ex.framing, 1u << 20,
                              monotonic() + cfg_.timeouts.upstream_header);
    reusable = upstream_keeps_alive(ex.response, ex.framing) && ex.reader->buffered() == 0;
  } catch (const std::exception&) {
  }
  ex.endpoint->release(std::move(ex.socket), reusable);
}

std::optional<double> Proxy::hedge_delay() const {
  std::optional<double> estimate = metrics::UPSTREAM_DURATION_ALL.quantile(cfg_.hedge.quantile);
  if (!estimate) return std::nullopt;
  return std::clamp(*estimate, cfg_.hedge.min_delay, cfg_.hedge.max_delay);
}

// ---------------------------------------------------------------------------
// Proxy: response relay
// ---------------------------------------------------------------------------

Task<Proxy::RelayOutcome> Proxy::relay_response(ClientConn& conn, const http::Request& req,
                                                Exchange& ex, bool client_keepalive, int attempts,
                                                bool hedged, const ResponseExtras& extras) {
  RelayOutcome out;
  const http::Response& resp = ex.response;
  if (resp.status == 101) {
    co_await tunnel(conn, req, ex);
    co_return out;
  }

  http::Headers headers = http::filtered_headers(resp);
  http::Framing framing = ex.framing;
  bool upstream_reusable = upstream_keeps_alive(resp, framing);
  bool dechunk = false;
  if (framing.kind == http::BodyKind::Chunked) {
    if (req.version == "HTTP/1.0") {
      // An HTTP/1.0 client cannot parse chunked: send the payload raw and
      // delimit it by closing.
      dechunk = true;
      client_keepalive = false;
    } else {
      headers.emplace_back("Transfer-Encoding", framing.codings);
    }
  } else if (framing.kind == http::BodyKind::Eof) {
    if (!framing.codings.empty() && req.version == "HTTP/1.1") {
      headers.emplace_back("Transfer-Encoding", framing.codings);
    }
    // Length unknown: the only honest framing left is "read until close".
    client_keepalive = false;
  }
  headers.emplace_back("Connection", client_keepalive ? "keep-alive" : "close");
  if (!extras.set_cookie.empty()) headers.emplace_back("Set-Cookie", extras.set_cookie);
  std::optional<Freshness> fresh;
  if (!extras.cache_key.empty()) {
    fresh = response_freshness(resp, framing);
    if (fresh && framing.length > cluster_.cache.max_object_bytes()) fresh.reset();
  }
  if (cfg_.debug_headers) {
    // "%.2fms" without printf's floating-point machinery, once per response.
    long long hundredths = std::llround(ex.elapsed * 1e5);
    std::string took = std::to_string(hundredths / 100) + "." + static_cast<char>('0' + hundredths / 10 % 10) +
                       static_cast<char>('0' + hundredths % 10) + "ms";
    headers.emplace_back("X-Upstream", ex.endpoint->name);
    headers.emplace_back("X-Upstream-Time", std::move(took));
    if (attempts > 1) headers.emplace_back("X-Attempts", std::to_string(attempts));
    if (hedged) headers.emplace_back("X-Hedged", "1");
    if (!extras.cache_key.empty()) headers.emplace_back("X-Cache", "MISS");
  }
  headers.emplace_back("Via", "1.1 " + cfg_.server_name);
  std::string head = http::encode_response(resp.status, headers, resp.reason);

  double deadline = monotonic() + cfg_.timeouts.upstream_body;
  uint64_t moved = 0;
  bool complete = false;
  try {
    bool whole = framing.kind == http::BodyKind::Length && ex.reader->buffered() >= framing.length;
    if (whole || fresh) {
      // The whole body arrived with the head (the common small response), or
      // it is about to be cached and has to be held anyway: one write, one
      // segment, no relay loop.  `fresh` implies a known, bounded length.
      std::string body = framing.kind == http::BodyKind::Length
                             ? co_await ex.reader->read_exact(static_cast<size_t>(framing.length), deadline)
                             : std::string();
      if (fresh) {
        auto entry = std::make_shared<CachedResponse>();
        entry->status = resp.status;
        entry->reason = resp.reason;
        entry->headers = http::filtered_headers(resp, {"content-length", "age"});
        entry->body = body;
        entry->stored_at = monotonic();
        entry->expires_at = entry->stored_at + fresh->ttl;
        entry->initial_age = fresh->age;
        cluster_.cache.put(extras.cache_key, std::move(entry));
      }
      head += body;
      moved = body.size();
      try {
        co_await conn.socket->write_all(head, deadline);
      } catch (const std::exception&) {
        throw http::SinkError("client write failed", false);
      }
    } else {
      try {
        co_await conn.socket->write_all(head, deadline);
      } catch (const std::exception&) {
        throw http::SinkError("client write failed", false);
      }
      moved = co_await http::relay_body(*ex.reader, http::Sink{conn.socket.get(), nullptr}, framing,
                                        std::nullopt, deadline, dechunk);
    }
    complete = true;
  } catch (const http::SinkError&) {
    out.client_gone = true;
  } catch (const std::exception&) {
    // Upstream broke mid-body.  The client already has a status line, so the
    // only signal left is an abrupt close -- which is exactly what a
    // truncated response should look like.
  }
  metrics::BYTES_OUT.inc(static_cast<double>(moved));
  if (!complete) co_return out;
  out.upstream_reusable = upstream_reusable;
  out.keep_alive = client_keepalive;
  co_return out;
}

Task<void> Proxy::tunnel(ClientConn& conn, const http::Request& req, Exchange& ex) {
  metrics::UPGRADES.inc();
  http::Headers headers = http::filtered_headers(ex.response);
  headers.emplace_back("Connection", "Upgrade");
  std::string upgrade = ex.response.get_or("upgrade");
  if (upgrade.empty()) upgrade = req.upgrade_target().value_or("websocket");
  headers.emplace_back("Upgrade", upgrade);
  bool ok = true;
  try {
    co_await conn.socket->write_all(http::encode_response(101, headers, ex.response.reason),
                                    monotonic() + cfg_.timeouts.client_header);
  } catch (const std::exception&) {
    ok = false;
  }
  if (!ok) co_return;

  // Both directions run until each has closed; the idle timeout catches a
  // peer that goes silent without closing (a dead NAT mapping).
  auto state = std::make_shared<TunnelState>();
  double idle = cfg_.timeouts.tunnel_idle;
  spawn(pump(ex.reader.get(), ex.socket, conn.socket, "downstream", state, idle));
  spawn(pump(&conn.reader, conn.socket, ex.socket, "upstream", state, idle));
  while (state->running > 0) {
    state->done.clear();
    co_await state->done.wait();
  }
}

Task<bool> Proxy::serve_cached(ClientConn& conn, const http::Request& req, const CachedResponse& cached,
                               bool client_keepalive) {
  http::Headers headers = cached.headers;
  if (cached.status != 204) headers.emplace_back("Content-Length", std::to_string(cached.body.size()));
  double age = cached.initial_age + (monotonic() - cached.stored_at);
  headers.emplace_back("Age", std::to_string(static_cast<long long>(age)));
  headers.emplace_back("Connection", client_keepalive ? "keep-alive" : "close");
  if (cfg_.debug_headers) headers.emplace_back("X-Cache", "HIT");
  headers.emplace_back("Via", "1.1 " + cfg_.server_name);
  std::string wire = http::encode_response(cached.status, headers, cached.reason);
  if (req.method != "HEAD") wire += cached.body;
  try {
    co_await conn.socket->write_all(wire, monotonic() + cfg_.timeouts.client_idle);
  } catch (const std::exception&) {
    co_return false;
  }
  metrics::BYTES_OUT.inc(req.method == "HEAD" ? 0.0 : static_cast<double>(cached.body.size()));
  co_return client_keepalive;
}

Task<void> Proxy::send_error(ClientConn& conn, int status, const std::string& reason,
                             const std::string& request_id, bool head_only,
                             const http::Headers& extra) {
  std::string body = "{\"error\":" + json_quote(reason) + ",\"status\":" + std::to_string(status) +
                     ",\"request_id\":" + json_quote(request_id) + "}\n";
  http::Headers headers = {{"X-Request-ID", request_id}};
  headers.insert(headers.end(), extra.begin(), extra.end());
  std::string wire = http::simple_response(status, body, "application/json", true, headers);
  if (head_only) wire.resize(wire.size() - body.size());
  try {
    co_await conn.socket->write_all(wire, monotonic() + 5.0);
  } catch (const std::exception&) {
  }
}

// ---------------------------------------------------------------------------
// Proxy: helpers
// ---------------------------------------------------------------------------

std::string Proxy::build_upstream_head(const http::Request& req, const http::Framing& framing,
                                       const std::string* body, const RequestCtx& ctx,
                                       const std::string& target) const {
  http::Headers headers = http::filtered_headers(
      req, {"content-length", "x-request-id", "x-forwarded-for", "x-forwarded-proto",
            "x-forwarded-host", "expect"});
  // We always speak HTTP/1.1 upstream, where Host is mandatory; an HTTP/1.0
  // client may have omitted it.  No authority known: empty (RFC 9110 7.2).
  if (!req.has("host")) headers.insert(headers.begin(), {"Host", ""});
  std::string chain;
  if (cfg_.trust_forwarded_for) {
    for (const std::string& v : req.get_all("x-forwarded-for")) {
      std::string_view t = http::trim(v);
      if (t.empty()) continue;
      if (!chain.empty()) chain += ", ";
      chain += t;
    }
  }
  // The hop we can vouch for is the peer that connected to us.
  if (!chain.empty()) chain += ", ";
  chain += ctx.peer_ip;
  headers.emplace_back("X-Forwarded-For", chain);
  headers.emplace_back("X-Forwarded-Proto", "http");
  headers.emplace_back("X-Forwarded-Host", req.get_or("host"));
  headers.emplace_back("X-Request-ID", ctx.request_id);

  if (std::optional<std::string> upgrade = req.upgrade_target()) {
    headers.emplace_back("Connection", "Upgrade");
    headers.emplace_back("Upgrade", *upgrade);
  } else {
    headers.emplace_back("Connection", cfg_.pool.enabled ? "keep-alive" : "close");
  }

  if (body != nullptr) {
    if (!body->empty() || req.method == "POST" || req.method == "PUT" || req.method == "PATCH") {
      headers.emplace_back("Content-Length", std::to_string(body->size()));
    }
  } else if (framing.kind == http::BodyKind::Chunked) {
    headers.emplace_back("Transfer-Encoding", "chunked");
  } else if (framing.kind == http::BodyKind::Length && framing.length) {
    headers.emplace_back("Content-Length", std::to_string(framing.length));
  }
  return http::encode_request(req.method, target, headers);
}

std::optional<std::string> cookie_value(const http::Request& req, std::string_view name) {
  for (const std::string& raw : req.get_all("cookie")) {
    std::string_view rest = raw;
    while (!rest.empty()) {
      size_t semi = rest.find(';');
      std::string_view part = http::trim(rest.substr(0, semi));
      size_t eq = part.find('=');
      if (eq != std::string_view::npos && part.substr(0, eq) == name && eq + 1 < part.size()) {
        return std::string(part.substr(eq + 1));
      }
      if (semi == std::string_view::npos) break;
      rest.remove_prefix(semi + 1);
    }
  }
  return std::nullopt;
}

std::optional<std::string> Proxy::affinity_key(const http::Request& req, const std::string& source_ip,
                                               const HashKeyConfig& spec) const {
  if (spec.source == "client_ip") return source_ip;
  if (spec.source == "header") {
    const std::string* v = req.get(spec.name);
    if (v != nullptr && !v->empty()) return *v;
    return std::nullopt;
  }
  if (spec.source == "path") return std::string(req.path());
  if (spec.source == "cookie") return cookie_value(req, spec.name);
  if (spec.source == "query") {
    std::optional<std::string> v = http::query_param(req.query(), spec.name);
    if (v && !v->empty()) return v;
  }
  return std::nullopt;
}

std::string Proxy::effective_client_ip(const http::Request& req, const std::string& peer_ip) const {
  if (!cfg_.trust_forwarded_for) return peer_ip;
  const std::string* forwarded = req.get("x-forwarded-for");
  if (forwarded == nullptr) return peer_ip;
  std::string_view first = http::trim(std::string_view(*forwarded).substr(0, forwarded->find(',')));
  return first.empty() ? peer_ip : std::string(first);
}

void Proxy::finish(const http::Request& req, const RequestCtx& ctx) {
  double elapsed = monotonic() - ctx.started;
  metrics::REQUESTS.inc({metric_method(req.method), std::to_string(ctx.status), ctx.endpoint});
  metrics::REQUEST_DURATION.observe({ctx.endpoint}, elapsed);
  if (!cfg_.log.access) return;
  if (cfg_.log.sample < 1.0 && elapsed < cfg_.log.slow_request && uniform(0.0, 1.0) >= cfg_.log.sample) {
    return;
  }
  char numbers[96];
  std::string line;
  line.reserve(256);
  std::snprintf(numbers, sizeof numbers, "{\"ts\":%.6f,\"ip\":", wall_time());
  line += numbers;
  line += json_quote(ctx.source_ip);
  line += ",\"method\":";
  line += json_quote(req.method);
  line += ",\"target\":";
  line += json_quote(req.target);
  std::snprintf(numbers, sizeof numbers, ",\"status\":%d,\"ms\":%.3f,\"upstream\":", ctx.status,
                elapsed * 1000.0);
  line += numbers;
  line += json_quote(ctx.endpoint);
  line += ",\"route\":";
  line += json_quote(ctx.route);
  if (!ctx.cache.empty()) {
    line += ",\"cache\":";
    line += json_quote(ctx.cache);
  }
  std::snprintf(numbers, sizeof numbers, ",\"attempts\":%d,\"hedged\":%s,\"rid\":", ctx.attempts,
                ctx.hedged ? "true" : "false");
  line += numbers;
  line += json_quote(ctx.request_id);
  std::snprintf(numbers, sizeof numbers, ",\"w\":%d}", worker_id_);
  line += numbers;
  log::access(line);
}

}  // namespace jdblance
