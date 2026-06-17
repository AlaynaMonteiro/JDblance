// Active health checking.
//
// Passive detection (the breaker) only learns a backend is dead by feeding it
// real requests.  Active probes find it first and, more importantly, are the
// only way to learn it has *recovered* without gambling live traffic.
//
//   * Jitter: N workers probing on the same 2 s boundary is a synchronised
//     herd against every backend.  Each loop starts at a random offset and
//     sleeps a jittered period.
//   * Hysteresis: `rise`/`fall` thresholds mean one dropped packet does not
//     eject a backend and one lucky probe does not restore it.
#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "config.hpp"
#include "endpoint.hpp"

namespace jdblance {

class HealthChecker {
 public:
  explicit HealthChecker(HealthConfig cfg) : cfg_(std::move(cfg)) {}
  ~HealthChecker() { stop(); }
  HealthChecker(const HealthChecker&) = delete;
  HealthChecker& operator=(const HealthChecker&) = delete;

  void start();
  // Stops every loop; each exits at its next wake-up (in-flight probes are
  // cancelled).  stop_and_wait() also waits until they have.
  void stop();
  Task<void> stop_and_wait();
  // New settings and endpoint set; running loops restart with them.
  void update(HealthConfig cfg, std::vector<EndpointPtr> endpoints);
  bool running() const noexcept { return running_; }

  // One probe, exposed for tests: (ok, detail).
  static Task<std::pair<bool, std::string>> probe(EndpointPtr endpoint, HealthConfig cfg,
                                                  std::shared_ptr<std::weak_ptr<Socket>> current);
  static void apply(Endpoint& endpoint, const HealthConfig& cfg, bool ok, const std::string& detail);

 private:
  struct LoopState {
    Event stop;
    Event finished;
    bool stopped = false;
    std::shared_ptr<std::weak_ptr<Socket>> probe_socket = std::make_shared<std::weak_ptr<Socket>>();
  };
  static Task<void> run(EndpointPtr endpoint, HealthConfig cfg, std::shared_ptr<LoopState> state);
  void launch();
  void halt_loops();

  HealthConfig cfg_;
  std::vector<EndpointPtr> endpoints_;
  std::vector<std::shared_ptr<LoopState>> loops_;
  std::vector<std::shared_ptr<LoopState>> stopping_;   // halted, not yet exited
  bool running_ = false;
};

}  // namespace jdblance
