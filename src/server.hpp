// Process supervision: listeners, workers, signals, graceful drain, reload.
//
// One event loop saturates one core, so `listen.workers > 1` forks N workers
// that each bind the *same* port with SO_REUSEPORT.  The kernel hashes every
// new connection's 4-tuple to one listening socket: accepts are distributed
// in-kernel, with no shared lock, no thundering herd, no handoff.  The
// supervisor forwards signals and respawns a worker that dies unexpectedly.
//
// SIGTERM drains: stop accepting, fail /readyz, finish in-flight requests
// (idle keep-alive connections are closed immediately), then exit; a second
// SIGTERM exits at once.  SIGHUP re-reads the config in place.
#pragma once

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "admin.hpp"
#include "config.hpp"
#include "loop.hpp"
#include "proxy.hpp"

namespace jdblance {

// Command-line settings that win over the file, on startup and on every reload.
struct CliOverrides {
  std::optional<std::string> listen_host;
  std::optional<int> listen_port;
  std::optional<std::string> policy;
  std::optional<int> workers;
  std::vector<EndpointConfig> endpoints;
  void apply(Config& cfg) const;
};

// Parses NAME=HOST:PORT[:WEIGHT] (IPv6 hosts in brackets).  Throws ConfigError.
EndpointConfig parse_endpoint_override(const std::string& spec);

class Worker {
 public:
  Worker(Config cfg, int worker_id, CliOverrides overrides);
  Worker(const Worker&) = delete;
  Worker& operator=(const Worker&) = delete;

  // Serves on `listen_fd` until a stop signal, then drains.  Returns the exit code.
  int run(int listen_fd);
  std::pair<bool, std::string> reload();

 private:
  static Task<void> accept_loop(Proxy* proxy, SocketPtr listener);
  Task<void> serve(SocketPtr listener);
  Task<void> reload_loop();
  Task<void> reaper_loop();
  Task<void> flush_loop();
  void on_signal(int signo);

  Loop loop_;
  Config cfg_;
  int worker_id_;
  CliOverrides overrides_;
  Cluster cluster_;
  Proxy proxy_;
  AdminServer admin_;
  bool stopping_ = false;
  Event stop_;
  Event reload_requested_;
  Event reaper_stop_;
  Event flush_stop_;
};

// Runs the configured number of workers; returns the process exit code.
int supervise(const Config& cfg, const CliOverrides& overrides);
// The `jdblance` command line.
int main_cli(int argc, char** argv);

}  // namespace jdblance
