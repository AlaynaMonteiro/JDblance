// A programmable origin server, for proving the load balancer works.
//
// Real backends fail in boring ways that are hard to reproduce on demand; this
// one fails on request:
//
//   POST /chaos?fail_rate=0.3&latency_ms=50&jitter_ms=20   (or the same as JSON)
//   POST /chaos?stall=1        accept requests, never answer
//   POST /chaos?healthy=0      fail /health while still serving /
//   POST /chaos?reset=1
//
// It also models a capacity: past `capacity` concurrent requests, latency grows
// the way a saturated thread pool's does, which is what makes adaptive
// concurrency limiting and CoDel observably do something.
//
// Routes: /  /health  /slow?ms=N  /echo  /cpu?ms=N  /stats  /info  /chaos  /metrics
// `/` answers JSON, or an HTML page when the client asks for text/html.
#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_set>

#include "http1.hpp"
#include "loop.hpp"
#include "metrics.hpp"

namespace jdblance {

class Origin : public std::enable_shared_from_this<Origin> {
 public:
  explicit Origin(std::string name, double base_latency = 0.0, double jitter = 0.0, int capacity = 0,
                  double fail_rate = 0.0);
  ~Origin();

  std::string name;
  double base_latency;
  double jitter;
  int capacity;
  double fail_rate;
  bool stall = false;
  bool healthy = true;
  int inflight = 0;
  int peak_inflight = 0;
  uint64_t requests = 0;
  uint64_t failures = 0;
  double started;

  // Listen on host:port (0 = ephemeral) and serve until close().
  void listen(const std::string& host, int port);
  int port() const noexcept { return port_; }
  // Stop listening *and* hang up on every open connection, as a crashed
  // process would.  listen() again to recover.
  void close();
  int active_connections() const noexcept { return static_cast<int>(conns_.size()); }

  double service_time();
  metrics::Registry registry;

 private:
  struct Reply {
    int status;
    std::string body;
    std::string content_type;
  };
  static Task<void> accept_loop(std::shared_ptr<Origin> self, SocketPtr listener);
  static Task<void> handle(std::shared_ptr<Origin> self, SocketPtr socket);
  Task<std::optional<Reply>> route(const http::Request& req, const std::string& body,
                                   BufReader& reader);
  Reply chaos(const http::Request& req, const std::string& body);
  Reply home(const http::Request& req);
  void observe(const std::string& route, int status, double seconds);

  SocketPtr listener_;
  int port_ = 0;
  std::unordered_set<Socket*> conns_;

  metrics::Counter requests_total_;
  metrics::Histogram duration_;
  metrics::Gauge inflight_gauge_;
  metrics::Gauge health_gauge_;
  metrics::Gauge chaos_gauge_;
};

std::string html_escape(std::string_view s);

}  // namespace jdblance
