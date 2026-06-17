// jdblance-demo: one-command proof that the resilience machinery does something.
//
// Four origins and the proxy in one process on ephemeral ports, steady
// open-loop traffic, and a timeline of things breaking:
//
//   1. baseline          all four healthy
//   2. origin0 fails     500 for every request
//   3. origin1 crawls    +400 ms of latency
//   4. origin2 refuses   listener closed and every connection dropped
//   5. recovery          faults cleared, origin2 rebound on its old port
//
// The number that matters is the client-visible error rate per phase.
#include <algorithm>
#include <cmath>
#include <cstdio>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "http1.hpp"
#include "origin.hpp"
#include "proxy.hpp"

using namespace jdblance;

namespace {

constexpr double kPhaseSeconds = 6.0;
constexpr double kRate = 300.0;

struct Phase {
  std::string name;
  std::string note;
  double seconds;
  std::vector<double> latencies;
  std::map<int, int> statuses;
  std::map<std::string, int> upstreams;
  int errors = 0;

  double error_rate() const {
    int total = errors, bad = errors;
    for (auto& [s, c] : statuses) {
      total += c;
      if (s >= 500) bad += c;
    }
    return total ? static_cast<double>(bad) / total : 0.0;
  }
  double percentile(double q) const {
    if (latencies.empty()) return std::nan("");
    std::vector<double> v = latencies;
    std::sort(v.begin(), v.end());
    return v[std::min(v.size() - 1, static_cast<size_t>(q * static_cast<double>(v.size())))];
  }
};

struct Inflight {
  int count = 0;
  Event drained;
};

Task<void> request_once(int port, Phase* phase, double intended, Inflight* inflight) {
  try {
    http::Headers headers = {{"Host", "demo"}};
    http::FetchResult r = co_await http::fetch("127.0.0.1", port, "GET", "/", headers, "", 5.0);
    phase->latencies.push_back(monotonic() - intended);
    ++phase->statuses[r.response.status];
    ++phase->upstreams[r.response.get_or("x-upstream", "-")];
  } catch (const std::exception&) {
    ++phase->errors;
  }
  if (--inflight->count == 0) inflight->drained.set();
}

// Open loop: keep firing at kRate whatever the server is doing.
Task<void> drive(int port, Phase* phase) {
  Inflight inflight;
  double next_at = monotonic();
  double deadline = next_at + phase->seconds;
  while (next_at < deadline) {
    next_at += 1.0 / kRate;
    double now = monotonic();
    if (next_at > now) co_await sleep_for(next_at - now);
    ++inflight.count;
    inflight.drained.clear();
    spawn(request_once(port, phase, next_at, &inflight));
  }
  if (inflight.count > 0) co_await inflight.drained.wait(monotonic() + 6.0);
  while (inflight.count > 0) co_await sleep_for(0.05);   // never leave a request pointing at us
}

void render(const std::vector<Phase>& phases) {
  std::printf("\n%s\n", std::string(78, '=').c_str());
  std::printf("%-22s%8s%9s%10s%10s  distribution\n", "phase", "rps", "errors", "p50", "p99");
  std::printf("%s\n", std::string(78, '-').c_str());
  for (const Phase& p : phases) {
    int total = 0;
    for (auto& [_, c] : p.statuses) total += c;
    std::string share;
    for (auto& [name, c] : p.upstreams) {
      double frac = static_cast<double>(c) / std::max(1, total);
      if (frac < 0.005) continue;
      char buf[64];
      std::snprintf(buf, sizeof buf, "%s%s:%.0f%%", share.empty() ? "" : "  ", name.c_str(), frac * 100);
      share += buf;
    }
    std::printf("%-22s%8.0f%8.2f%%%9.1fms%9.1fms  %s\n", p.name.c_str(), total / p.seconds,
                p.error_rate() * 100, p.percentile(.5) * 1000, p.percentile(.99) * 1000, share.c_str());
  }
  std::printf("%s\n", std::string(78, '=').c_str());
  const Phase* worst = &phases[0];
  for (const Phase& p : phases) {
    if (p.error_rate() > worst->error_rate()) worst = &p;
  }
  std::printf("\nworst client-visible error rate across all phases: %.2f%% (during '%s')\n",
              worst->error_rate() * 100, worst->name.c_str());
  std::printf("...while three of the four origins were, in turn, failing every request, "
              "crawling, or refusing connections outright.\n\n");
}

Task<void> accept_loop(Proxy* proxy, SocketPtr listener) {
  for (;;) {
    SocketPtr s;
    try {
      s = co_await accept_one(*listener);
    } catch (const std::exception&) {
      co_return;
    }
    spawn(proxy->handle_client(std::move(s)));
  }
}

struct Script {
  const char* name;
  const char* note;
  std::function<void()> action;
  double scale;
};

Task<void> demo(std::vector<std::shared_ptr<Origin>>* origins, Cluster* cluster, Proxy* proxy,
                SocketPtr listener, std::vector<Phase>* phases) {
  int port = listener->local_port();
  int origin2_port = (*origins)[2]->port();
  std::vector<Script> script = {
      {"1. baseline", "all healthy", nullptr, 1.0},
      {"2. origin0 fails", "returns 500 for every request", [&] { (*origins)[0]->fail_rate = 1.0; }, 1.0},
      {"3. origin1 crawls", "+400 ms of latency", [&] { (*origins)[1]->base_latency = 0.4; }, 1.0},
      {"4. origin2 refuses", "listener closed, connections dropped", [&] { (*origins)[2]->close(); }, 1.0},
      // Longer, because recovery is deliberately not instant: health checks
      // need `rise` passes and a slow backend's EWMA has to decay.
      {"5. recovery", "faults cleared, origin2 rebound",
       [&] {
         for (auto& o : *origins) {
           o->fail_rate = 0.0;
           o->base_latency = 0.003;
           o->healthy = true;
         }
         (*origins)[2]->listen("127.0.0.1", origin2_port);
       },
       2.0},
  };
  for (const Script& step : script) {
    if (step.action) step.action();
    phases->push_back(Phase{step.name, step.note, kPhaseSeconds * step.scale, {}, {}, {}, 0});
    std::printf("  %-20s %s\n", step.name, step.note);
    std::fflush(stdout);
    co_await drive(port, &phases->back());
  }
  listener->close();
  proxy->close_all();
  for (auto& o : *origins) o->close();
  co_await cluster->health.stop_and_wait();
  for (const EndpointPtr& e : cluster->endpoints()) e->pool.clear();
  co_await sleep_for(0.1);
}

}  // namespace

int main() {
  Loop loop;
  std::vector<std::shared_ptr<Origin>> origins;
  Json endpoints = Json::array();
  for (int i = 0; i < 4; ++i) {
    auto o = std::make_shared<Origin>("origin" + std::to_string(i), 0.003, 0.002);
    o->listen("127.0.0.1", 0);
    Json e = Json::object();
    e["name"] = o->name;
    e["address"] = "127.0.0.1:" + std::to_string(o->port());
    endpoints.push_back(e);
    origins.push_back(o);
  }
  Json data = Json::parse(R"({
    "listen": {"host": "127.0.0.1", "port": 0},
    "admin": {"enabled": false},
    "policy": "p2c_ewma",
    "health": {"enabled": true, "interval": 1.0, "fall": 2, "rise": 1},
    "breaker": {"consecutive_failures": 5, "min_requests": 10, "cooldown": 3.0},
    "hedge": {"enabled": true},
    "log": {"access": false}
  })");
  data["endpoints"] = endpoints;
  Config cfg = Config::from_json(data);
  Cluster cluster(cfg);
  Proxy proxy(cfg, cluster);
  auto listener = std::make_shared<Socket>(listen_tcp("127.0.0.1", 0, 1024, false));
  spawn(accept_loop(&proxy, listener));
  cluster.start_health();

  std::printf("proxy on 127.0.0.1:%d -> %zu origins, policy=%s, %.0f rps open loop, %.0fs per phase\n",
              listener->local_port(), origins.size(), cfg.policy.c_str(), kRate, kPhaseSeconds);
  std::vector<Phase> phases;
  loop.run_until_complete(demo(&origins, &cluster, &proxy, listener, &phases));

  render(phases);
  for (const EndpointPtr& e : cluster.endpoints()) {
    Json s = e->snapshot();
    std::printf("  %-10s requests=%-7s failures=%-5s breaker=%-10s ewma=%sms\n", e->name.c_str(),
                format_number(s.find("requests")->as_number()).c_str(),
                format_number(s.find("failures")->as_number()).c_str(), e->breaker.state_name(),
                format_number(s.find("ewma_ms")->as_number()).c_str());
  }
  std::printf("\n");
  return 0;
}
