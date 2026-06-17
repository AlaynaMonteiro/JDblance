#include "health.hpp"

#include <algorithm>

#include "balancer.hpp"
#include "http1.hpp"
#include "metrics.hpp"

namespace jdblance {

void HealthChecker::start() {
  if (running_) return;
  running_ = true;
  launch();
}

void HealthChecker::launch() {
  for (const auto& endpoint : endpoints_) {
    auto state = std::make_shared<LoopState>();
    loops_.push_back(state);
    spawn(run(endpoint, cfg_, state));
  }
}

void HealthChecker::halt_loops() {
  for (auto& state : loops_) {
    state->stopped = true;
    state->stop.set();
    if (auto sock = state->probe_socket->lock()) sock->cancel();
    stopping_.push_back(std::move(state));
  }
  loops_.clear();
  // Forget loops that have already exited.
  stopping_.erase(std::remove_if(stopping_.begin(), stopping_.end(),
                                 [](const auto& s) { return s->finished.is_set(); }),
                  stopping_.end());
}

void HealthChecker::stop() {
  running_ = false;
  halt_loops();
}

Task<void> HealthChecker::stop_and_wait() {
  stop();
  std::vector<std::shared_ptr<LoopState>> pending = std::move(stopping_);
  stopping_.clear();
  for (auto& state : pending) co_await state->finished.wait();
}

void HealthChecker::update(HealthConfig cfg, std::vector<EndpointPtr> endpoints) {
  cfg_ = std::move(cfg);
  endpoints_ = std::move(endpoints);
  if (!running_) return;
  halt_loops();
  launch();
}

Task<void> HealthChecker::run(EndpointPtr endpoint, HealthConfig cfg,
                              std::shared_ptr<LoopState> state) {
  // De-synchronise from every other worker and loop.
  bool stopped = co_await state->stop.wait(monotonic() + uniform(0.0, cfg.interval));
  while (!stopped && !state->stopped) {
    double started = monotonic();
    std::pair<bool, std::string> result;
    try {
      result = co_await probe(endpoint, cfg, state->probe_socket);
    } catch (const std::exception& e) {
      result = {false, e.what()};
    }
    // A loop that was replaced while probing must not touch shared state:
    // its endpoint may have left the config (and its metrics with it).
    if (state->stopped) break;
    apply(*endpoint, cfg, result.first, result.second);
    double elapsed = monotonic() - started;
    // Sleep the rest of the period, jittered so probes never re-converge.
    double pause = std::max(0.05, cfg.interval - elapsed) * uniform(0.9, 1.1);
    stopped = co_await state->stop.wait(monotonic() + pause);
  }
  state->finished.set();
}

Task<std::pair<bool, std::string>> HealthChecker::probe(
    EndpointPtr endpoint, HealthConfig cfg, std::shared_ptr<std::weak_ptr<Socket>> current) {
  double deadline = monotonic() + cfg.timeout;
  std::string failure;
  SocketPtr sock;
  try {
    std::vector<Address> addrs = co_await resolve(endpoint->host, endpoint->port, deadline);
    sock = co_await connect_any(std::move(addrs), deadline);
    *current = sock;
    http::Headers headers = {
        {"Host", endpoint->address()},
        {"User-Agent", "jdblance-healthcheck/1.0"},
        {"Accept", "*/*"},
        {"Connection", "close"},
    };
    co_await sock->write_all(http::encode_request("GET", cfg.path, headers), deadline);
    BufReader reader(sock);
    http::Response response = co_await http::read_response(reader, deadline);
    if (response.status != cfg.expected_status) {
      failure = "status " + std::to_string(response.status);
    } else if (cfg.expected_body) {
      std::string body;
      co_await http::relay_body(reader, http::Sink{nullptr, &body},
                                http::response_framing("GET", response), 64 * 1024, deadline, true);
      if (body.find(*cfg.expected_body) == std::string::npos) failure = "body mismatch";
    }
  } catch (const TimeoutError&) {
    failure = "timeout";
  } catch (const CancelledError&) {
    failure = "cancelled";
  } catch (const http::ProtocolError& e) {
    failure = "protocol: " + e.message;
  } catch (const ConnectionClosed&) {
    failure = "connection closed";
  } catch (const IoError& e) {
    failure = e.what();
  }
  if (sock) sock->close();
  current->reset();
  if (!failure.empty()) co_return std::make_pair(false, failure);
  co_return std::make_pair(true, std::string("ok"));
}

void HealthChecker::apply(Endpoint& endpoint, const HealthConfig& cfg, bool ok,
                          const std::string& detail) {
  metrics::HEALTH_CHECKS.inc({endpoint.name, ok ? "pass" : "fail"});
  if (ok) {
    endpoint.fall_count = 0;
    ++endpoint.rise_count;
    if (!endpoint.healthy && endpoint.rise_count >= cfg.rise) {
      endpoint.healthy = true;
      endpoint.rise_count = 0;
      // A backend that just came back should not inherit an open breaker
      // from the outage that took it away.
      endpoint.breaker.force_close();
      endpoint.last_error.clear();
    }
  } else {
    endpoint.rise_count = 0;
    ++endpoint.fall_count;
    if (endpoint.healthy && endpoint.fall_count >= cfg.fall) {
      endpoint.healthy = false;
      endpoint.fall_count = 0;
      endpoint.last_error = detail;
      endpoint.pool.clear();   // sockets to a dead box are worthless
      metrics::POOL_IDLE.set({endpoint.name}, 0);
    }
  }
  metrics::ENDPOINT_UP.set({endpoint.name}, endpoint.healthy ? 1 : 0);
  metrics::ENDPOINT_BREAKER.set({endpoint.name}, static_cast<double>(endpoint.breaker.state()));
}

}  // namespace jdblance
