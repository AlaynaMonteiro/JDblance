// Admin / control plane, on its own listener so operating the proxy never
// competes with -- or is blocked by -- the traffic it is struggling to serve:
// the moment you need /drain, the data port is on fire.
//
//   GET  /                          live dashboard
//   GET  /metrics                   Prometheus exposition
//   GET  /healthz                   liveness
//   GET  /readyz                    readiness: 503 when draining or no upstream is usable
//   GET  /stats                     cluster state as JSON
//   GET  /config                    effective configuration
//   POST /reload                    re-read the config file
//   POST /drain                     drain this worker (rolling deploys)
//   POST /endpoints/<name>/drain|enable|disable
//   POST /endpoints/<name>/weight?value=N
//   POST /endpoints/<name>/breaker/open|close
//
// Readiness fails when no upstream is usable -- panic mode included -- so an
// orchestrator pulls this instance from *its* rotation instead of routing to a
// proxy that can only emit errors.
#pragma once

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "config.hpp"
#include "http1.hpp"
#include "proxy.hpp"

namespace jdblance {

class AdminServer {
 public:
  using ReloadHook = std::function<std::pair<bool, std::string>()>;

  AdminServer(const Config& cfg, Cluster& cluster, Proxy& proxy, ReloadHook reload = nullptr,
              int worker_id = 0);
  ~AdminServer();
  AdminServer(const AdminServer&) = delete;
  AdminServer& operator=(const AdminServer&) = delete;

  void start(const std::string& host, int port);   // throws IoError
  void stop();
  int port() const;

  struct Reply {
    int status;
    std::string body;
    std::string content_type;
  };
  Reply route(const http::Request& req);
  Json stats();

 private:
  struct Shared {
    AdminServer* server;   // null once stopped: late connections just hang up
  };
  static Task<void> accept_loop(std::shared_ptr<Shared> shared, SocketPtr listener);
  static Task<void> handle(std::shared_ptr<Shared> shared, SocketPtr socket);
  Reply endpoint_action(const std::string& name, const std::vector<std::string>& action,
                        const http::Request& req);

  const Config& cfg_;
  Cluster& cluster_;
  Proxy& proxy_;
  ReloadHook reload_;
  int worker_id_;
  double started_;
  std::shared_ptr<Shared> shared_;
  SocketPtr listener_;
};

}  // namespace jdblance
