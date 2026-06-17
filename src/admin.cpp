#include "admin.hpp"

#include <limits>

#include "dashboard.hpp"
#include "metrics.hpp"

namespace jdblance {

namespace {
constexpr const char* kJson = "application/json";

AdminServer::Reply json_reply(int status, const Json& body) {
  return {status, body.dump() + "\n", kJson};
}

AdminServer::Reply error_reply(int status, const std::string& message) {
  Json body = Json::object();
  body["error"] = message;
  return json_reply(status, body);
}

Json ms_or_null(std::optional<double> seconds) {
  return seconds ? Json(round_to(*seconds * 1000.0, 3)) : Json();
}

bool parse_weight(const std::string& s, int* out) {
  if (s.empty() || s.size() > 7) return false;
  int v = 0;
  for (char c : s) {
    if (c < '0' || c > '9') return false;
    v = v * 10 + (c - '0');
  }
  if (v < 1 || v > 1000000) return false;
  *out = v;
  return true;
}
}  // namespace

AdminServer::AdminServer(const Config& cfg, Cluster& cluster, Proxy& proxy, ReloadHook reload,
                         int worker_id)
    : cfg_(cfg),
      cluster_(cluster),
      proxy_(proxy),
      reload_(std::move(reload)),
      worker_id_(worker_id),
      started_(wall_time()),
      shared_(std::make_shared<Shared>(Shared{this})) {}

AdminServer::~AdminServer() { stop(); }

void AdminServer::start(const std::string& host, int port) {
  int fd = listen_tcp(host, port, 64, false);
  listener_ = std::make_shared<Socket>(fd);
  spawn(accept_loop(shared_, listener_));
}

void AdminServer::stop() {
  shared_->server = nullptr;
  if (listener_) listener_->close();
}

int AdminServer::port() const { return listener_ ? listener_->local_port() : -1; }

Task<void> AdminServer::accept_loop(std::shared_ptr<Shared> shared, SocketPtr listener) {
  for (;;) {
    SocketPtr socket;
    try {
      socket = co_await accept_one(*listener);
    } catch (const std::exception&) {
      co_return;   // listener closed
    }
    if (shared->server == nullptr) {
      socket->close();
      co_return;
    }
    spawn(handle(shared, std::move(socket)));
  }
}

Task<void> AdminServer::handle(std::shared_ptr<Shared> shared, SocketPtr socket) {
  std::string wire;
  try {
    BufReader reader(socket);
    double deadline = monotonic() + 10.0;
    http::Request req = co_await http::read_request(reader, deadline);
    http::Framing framing = http::request_framing(req);
    if (!framing.empty()) {
      co_await http::relay_body(reader, http::Sink{}, framing, 1u << 20, deadline);
    }
    if (shared->server != nullptr) {
      Reply reply = shared->server->route(req);
      wire = http::simple_response(reply.status, reply.body, reply.content_type, true);
      if (req.method == "HEAD") wire.resize(wire.size() - reply.body.size());
    }
  } catch (const http::ProtocolError& e) {
    wire = http::simple_response(e.status, e.message + "\n");
  } catch (const std::exception& e) {
    // Timeouts and resets: nothing to say.  Anything else is a bug, but the
    // admin plane must never take the process down with it.
    if (!dynamic_cast<const TimeoutError*>(&e) && !dynamic_cast<const IoError*>(&e) &&
        !dynamic_cast<const ConnectionClosed*>(&e) && !dynamic_cast<const CancelledError*>(&e)) {
      wire = http::simple_response(500, std::string("internal error: ") + e.what() + "\n");
    }
  }
  if (!wire.empty()) {
    try {
      co_await socket->write_all(wire, monotonic() + 10.0);
    } catch (const std::exception&) {
    }
  }
  socket->close();
}

AdminServer::Reply AdminServer::route(const http::Request& req) {
  std::string path(req.path());
  while (path.size() > 1 && path.back() == '/') path.pop_back();
  const std::string& method = req.method;

  if (method == "GET" || method == "HEAD") {
    if (path == "/") return {200, kDashboardHtml, "text/html; charset=utf-8"};
    if (path == "/metrics") {
      return {200, metrics::registry().render(), "text/plain; version=0.0.4; charset=utf-8"};
    }
    if (path == "/healthz") return {200, "{\"status\":\"ok\"}\n", kJson};
    if (path == "/readyz") {
      size_t usable = cluster_.usable();
      Json body = Json::object();
      if (proxy_.draining()) {
        body["status"] = "draining";
        return json_reply(503, body);
      }
      body["status"] = usable ? "ready" : "no_upstreams";
      body["healthy"] = usable;
      return json_reply(usable ? 200 : 503, body);
    }
    if (path == "/stats") return json_reply(200, stats());
    if (path == "/config") return {200, cfg_.to_json().dump(2) + "\n", kJson};
    return error_reply(404, "not found");
  }
  if (method != "POST") return error_reply(405, "method not allowed");

  if (path == "/reload") {
    if (!reload_) return error_reply(501, "reload not available");
    auto [ok, detail] = reload_();
    metrics::CONFIG_RELOADS.inc({ok ? "ok" : "error"});
    Json body = Json::object();
    body["reloaded"] = ok;
    body["detail"] = detail;
    return json_reply(ok ? 200 : 400, body);
  }
  if (path == "/drain") {
    proxy_.start_drain();
    Json body = Json::object();
    body["draining"] = true;
    return json_reply(200, body);
  }

  std::vector<std::string> parts;
  size_t start = 1;
  while (start <= path.size()) {
    size_t slash = path.find('/', start);
    if (slash == std::string::npos) slash = path.size();
    if (slash > start) parts.push_back(path.substr(start, slash - start));
    start = slash + 1;
  }
  if (parts.size() >= 3 && parts[0] == "endpoints") {
    return endpoint_action(parts[1], std::vector<std::string>(parts.begin() + 2, parts.end()), req);
  }
  return error_reply(404, "not found");
}

AdminServer::Reply AdminServer::endpoint_action(const std::string& name,
                                                const std::vector<std::string>& action,
                                                const http::Request& req) {
  EndpointPtr endpoint = cluster_.find(name);
  if (!endpoint) return error_reply(404, "unknown endpoint");
  const std::string& verb = action[0];
  if (verb == "drain" && action.size() == 1) {
    endpoint->draining = true;
    endpoint->pool.clear();
  } else if (verb == "enable" && action.size() == 1) {
    endpoint->draining = false;
    endpoint->admin_enabled = true;
    endpoint->breaker.force_close();
  } else if (verb == "disable" && action.size() == 1) {
    endpoint->admin_enabled = false;
    endpoint->pool.clear();
  } else if (verb == "weight" && action.size() == 1) {
    std::optional<std::string> value = http::query_param(req.query(), "value");
    int weight = 0;
    if (!value || !parse_weight(*value, &weight)) {
      return error_reply(400, "weight requires ?value=<1..1000000>");
    }
    endpoint->weight = weight;
  } else if (verb == "breaker" && action.size() == 2) {
    if (action[1] == "open") {
      endpoint->breaker.force_open(cfg_.breaker.cooldown);
    } else if (action[1] == "close") {
      endpoint->breaker.force_close();
    } else {
      return error_reply(400, "breaker takes open|close");
    }
  } else {
    return error_reply(404, "unknown action");
  }
  metrics::POOL_IDLE.set({endpoint->name}, static_cast<double>(endpoint->pool.size()));
  metrics::ENDPOINT_BREAKER.set({endpoint->name}, static_cast<double>(endpoint->breaker.state()));
  return json_reply(200, endpoint->snapshot());
}

Json AdminServer::stats() {
  Json out = cluster_.snapshot();
  out["worker"] = worker_id_;
  out["uptime_seconds"] = round_to(wall_time() - started_, 1);
  out["connections"] = proxy_.connections();
  out["draining"] = proxy_.draining();

  Json totals = Json::object();
  totals["requests"] = metrics::REQUESTS.total();
  totals["shed"] = metrics::SHED.total();
  totals["retries"] = metrics::RETRIES.total();
  totals["hedges"] = metrics::HEDGES.value();
  totals["hedge_wins"] = metrics::HEDGE_WINS.value();
  totals["upgrades"] = metrics::UPGRADES.value();
  totals["bytes_in"] = metrics::BYTES_IN.value();
  totals["bytes_out"] = metrics::BYTES_OUT.value();
  out["totals"] = std::move(totals);

  Json latency = Json::object();
  latency["p50"] = ms_or_null(metrics::UPSTREAM_DURATION_ALL.quantile(0.50));
  latency["p90"] = ms_or_null(metrics::UPSTREAM_DURATION_ALL.quantile(0.90));
  latency["p99"] = ms_or_null(metrics::UPSTREAM_DURATION_ALL.quantile(0.99));
  out["latency_ms"] = std::move(latency);

  Json classes = Json::object();
  for (const auto& [labels, count] : metrics::REQUESTS.children()) {
    const std::string& status = labels[1];
    std::string bucket = !status.empty() && status[0] >= '0' && status[0] <= '9'
                             ? std::string(1, status[0]) + "xx"
                             : "other";
    Json& slot = classes[bucket];
    slot = Json((slot.is_null() ? 0.0 : slot.as_number()) + count);
  }
  out["status_classes"] = std::move(classes);
  return out;
}

}  // namespace jdblance
