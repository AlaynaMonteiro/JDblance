// The datapath: accept, route, admit, select, forward, retry, hedge, relay.
//
//   accept -> parse head -> rate limit -> route (host + prefix) -> route rate
//          limit -> cache lookup -> admission (concurrency + CoDel)
//          -> affinity key / sticky cookie -> select within the route's
//          upstream -> [attempt: connect, send, read head] -> relay body
//          (storing it if cacheable) -> keep-alive decision
//
// Retries happen only while nothing has been written to the client: once one
// response byte is on the wire the request is committed.  Bodies of known
// length are buffered up to `retry.buffer_bytes` so a POST stays retryable --
// but only for failures where the origin cannot have seen it (connect).  An
// idempotent request may be retried after any failure.
//
// Hedging is a p99 tool, not a throughput tool: a second copy of an idempotent
// bodyless request goes out only once the first has outlived the cluster's
// observed p99, the backup is chosen only at that moment, and it draws from
// the retry budget, so a slow *cluster* cannot hedge itself into a spiral.
//
// Panic mode: below `panic_threshold` healthy endpoints, health state is
// ignored and traffic is sprayed across everything administratively in.  When
// most of the fleet looks unhealthy the health signal is the likeliest thing
// that is broken (Envoy's behaviour).
#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "balancer.hpp"
#include "config.hpp"
#include "endpoint.hpp"
#include "health.hpp"
#include "http1.hpp"
#include "limits.hpp"
#include "loop.hpp"
#include "routing.hpp"
#include "upstream.hpp"

namespace jdblance {

inline constexpr const char* kVersion = "1.0.0";

class UpstreamError : public std::runtime_error {
 public:
  UpstreamError(std::string reason, bool retryable, int status = 502)
      : std::runtime_error(reason), reason(std::move(reason)), retryable(retryable), status(status) {}
  std::string reason;
  bool retryable;
  int status;
};

// The client failed while its body was being streamed upstream.  Not the
// backend's fault, so it is never recorded against the endpoint -- otherwise a
// handful of bad clients could trip the breaker on a healthy backend.
class ClientError : public std::runtime_error {
 public:
  ClientError(const std::string& what, int status) : std::runtime_error(what), status(status) {}
  int status;   // 0: the client is gone, nothing to send
};

// One upstream attempt, paused just after the response head.
struct Exchange {
  EndpointPtr endpoint;
  SocketPtr socket;
  std::unique_ptr<BufReader> reader;
  http::Response response;
  http::Framing framing;
  bool reused = false;
  double started = 0;
  double elapsed = 0;
  // Drop a losing hedge: mid-response, so the socket cannot be pooled.
  void abandon();
};

// A route and the state it owns.  Held by shared_ptr: a request keeps its
// route across co_awaits, and a reload may replace the table meanwhile.
struct Route {
  RouteConfig spec;
  std::shared_ptr<Upstream> upstream;
  std::unique_ptr<KeyedRateLimiter> limiter;   // null: no per-route limit
};
using RoutePtr = std::shared_ptr<Route>;

class Cluster {
 public:
  explicit Cluster(const Config& cfg);

  // (Re)build upstreams, endpoints and routes, carrying live state (breaker,
  // EWMA, pool, rate-limit buckets) over for survivors, and apply every
  // live-tunable setting.  Used by SIGHUP reload.
  void apply_config(const Config& cfg);
  // Run health checks per cfg.health.enabled from now on (and across reloads).
  void start_health();

  // The first route that matches, else the implicit route to "default", else null.
  RoutePtr route(const http::Request& req) const;
  std::shared_ptr<Upstream> upstream(std::string_view name) const;
  const std::vector<std::shared_ptr<Upstream>>& upstreams() const noexcept { return upstreams_; }
  // The first upstream ("default" when top-level endpoints exist).
  Upstream& primary() const noexcept { return *upstreams_.front(); }

  // Every endpoint of every upstream.
  const std::vector<EndpointPtr>& endpoints() const noexcept { return endpoints_; }
  EndpointPtr find(std::string_view name) const;
  size_t usable() const;   // endpoints that would be chosen outside panic mode
  // Every upstream has a usable endpoint and none is in panic mode.
  bool ready() const;
  // Shortcuts to the primary upstream, for single-pool setups.
  std::pair<std::vector<EndpointPtr>, bool> candidates(double now = monotonic()) const {
    return primary().candidates(now);
  }
  Balancer& balancer() noexcept { return primary().balancer(); }
  Json snapshot();

  RetryBudget retry_budget;
  std::unique_ptr<KeyedRateLimiter> rate_limiter;
  std::unique_ptr<AdmissionController> admission;
  HealthChecker health;
  ResponseCache cache;

 private:
  std::optional<double> rate_burst_;
  std::vector<std::shared_ptr<Upstream>> upstreams_;
  std::vector<RoutePtr> routes_;
  RoutePtr default_route_;
  std::vector<EndpointPtr> endpoints_;
  bool manage_health_ = false;
  bool health_enabled_ = false;
};

class Proxy {
 public:
  Proxy(const Config& cfg, Cluster& cluster, int worker_id = 0);

  // One client connection, start to finish.  Spawn it per accepted socket.
  Task<void> handle_client(SocketPtr socket);

  int connections() const noexcept { return static_cast<int>(conns_.size()); }
  bool draining() const noexcept { return draining_; }
  // Serve in-flight requests with `Connection: close` and close idle
  // keep-alive connections now, so draining does not wait out client_idle.
  void start_drain();
  void close_all();

 private:
  struct ClientConn {
    SocketPtr socket;
    BufReader reader;
    std::string peer_ip;
    int served = 0;
  };
  struct RequestCtx {
    double started = 0;
    std::string request_id;
    std::string source_ip;
    std::string peer_ip;
    std::string endpoint = "-";
    std::string route = "-";
    std::string cache;   // "", "hit", "miss"
    int status = 0;
    int attempts = 0;
    bool hedged = false;
  };
  // What relay_response adds beyond proxying: a sticky cookie to set, and the
  // key to store the response under if it turns out to be cacheable.
  struct ResponseExtras {
    std::string set_cookie;
    std::string cache_key;
  };
  struct AttemptInput {
    std::string method;
    bool idempotent = false;
    bool upgrade = false;
    std::shared_ptr<const std::string> head;
    std::shared_ptr<const std::string> body;   // null: stream from `client`
    http::Framing framing;
    BufReader* client = nullptr;
  };
  struct CancelToken {
    bool cancelled = false;
    std::weak_ptr<Socket> socket;
    void cancel();
  };
  struct Dispatched {
    Exchange exchange;
    int attempts;
    bool hedged;
  };
  struct RelayOutcome {
    bool upstream_reusable = false;
    bool keep_alive = false;
    bool client_gone = false;
  };
  struct HedgeRace;

  Task<bool> serve(ClientConn& conn, http::Request& req);
  Task<bool> forward(ClientConn& conn, http::Request& req, http::Framing framing, RequestCtx& ctx,
                     bool client_keepalive, RoutePtr route, std::string cache_key);
  Task<bool> serve_cached(ClientConn& conn, const http::Request& req, const CachedResponse& cached,
                          bool client_keepalive);
  Task<Dispatched> dispatch(ClientConn& conn, const http::Request& req, http::Framing framing,
                            std::shared_ptr<const std::string> body,
                            const std::optional<std::string>& key, std::shared_ptr<const std::string> head,
                            std::shared_ptr<Upstream> upstream, std::string preferred);
  Task<std::pair<Exchange, bool>> hedged_attempt(EndpointPtr primary, AttemptInput input,
                                                 const std::optional<std::string>& key,
                                                 std::vector<std::string>& tried, Upstream& upstream);
  static Task<void> race_runner(Proxy* self, std::shared_ptr<HedgeRace> race, int index,
                                EndpointPtr endpoint, AttemptInput input);
  Task<Exchange> attempt(EndpointPtr endpoint, AttemptInput input, std::shared_ptr<CancelToken> token);
  Task<void> discard(Exchange exchange);
  Task<RelayOutcome> relay_response(ClientConn& conn, const http::Request& req, Exchange& ex,
                                    bool client_keepalive, int attempts, bool hedged,
                                    const ResponseExtras& extras);
  Task<void> tunnel(ClientConn& conn, const http::Request& req, Exchange& ex);
  Task<void> send_error(ClientConn& conn, int status, const std::string& reason,
                        const std::string& request_id, bool head_only,
                        const http::Headers& extra);

  std::optional<double> hedge_delay() const;
  std::string build_upstream_head(const http::Request& req, const http::Framing& framing,
                                  const std::string* body, const RequestCtx& ctx,
                                  const std::string& target) const;
  std::optional<std::string> affinity_key(const http::Request& req, const std::string& source_ip,
                                          const HashKeyConfig& spec) const;
  std::string effective_client_ip(const http::Request& req, const std::string& peer_ip) const;
  void finish(const http::Request& req, const RequestCtx& ctx);

  const Config& cfg_;
  Cluster& cluster_;
  int worker_id_;
  bool draining_ = false;
  std::unordered_set<ClientConn*> conns_;
  std::unordered_set<ClientConn*> idle_;
};

// Helpers shared with the admin plane and tests.
bool client_wants_keepalive(const http::Request& req);
std::string metric_method(std::string_view method);
// Value of cookie `name` in the request's Cookie headers, nullopt if absent.
std::optional<std::string> cookie_value(const http::Request& req, std::string_view name);

}  // namespace jdblance
