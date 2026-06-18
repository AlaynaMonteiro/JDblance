#include "upstream.hpp"

#include <algorithm>
#include <cmath>
#include <cstdio>

#include "log.hpp"
#include "metrics.hpp"

namespace jdblance {

namespace {
std::string balancer_signature(const UpstreamConfig& spec, const Config& cfg) {
  return spec.policy + "|" + std::to_string(cfg.vnodes) + "|" + format_number(cfg.bounded_load_factor) +
         "|" + std::to_string(cfg.maglev_table_size);
}
}  // namespace

void Upstream::apply(const UpstreamConfig& spec, const Config& cfg,
                     const std::vector<EndpointPtr>& previous) {
  spec_ = spec;
  panic_threshold_ = cfg.panic_threshold;
  slow_start_ = cfg.slow_start;
  if (!(outlier_.enabled && cfg.outlier.enabled && outlier_.interval == cfg.outlier.interval)) {
    next_outlier_check_ = monotonic() + cfg.outlier.interval;
  }
  outlier_ = cfg.outlier;

  std::string signature = balancer_signature(spec, cfg);
  if (signature != balancer_signature_) {
    balancer_ = build_balancer(spec.policy, cfg.vnodes, cfg.bounded_load_factor, cfg.maglev_table_size);
    balancer_signature_ = signature;
  }

  double now = monotonic();
  int max_idle = cfg.pool.enabled ? cfg.pool.max_idle_per_endpoint : 0;
  std::vector<EndpointPtr> rebuilt;
  rebuilt.reserve(spec.endpoints.size());
  for (const EndpointConfig& e : spec.endpoints) {
    auto old = std::find_if(previous.begin(), previous.end(),
                            [&](const EndpointPtr& p) { return p->name == e.name; });
    if (old != previous.end() && (*old)->address() == e.address) {
      EndpointPtr survivor = *old;
      survivor->weight = std::max(1, e.weight);
      survivor->admin_enabled = e.enabled;
      survivor->ewma_tau = cfg.ewma_tau;
      survivor->breaker.configure(cfg.breaker);
      survivor->pool.configure(max_idle, cfg.pool.idle_timeout);
      survivor->upstream = name_;
      rebuilt.push_back(std::move(survivor));
      continue;
    }
    auto endpoint = std::make_shared<Endpoint>(e.name, e.host(), e.port(), e.weight,
                                               CircuitBreaker(cfg.breaker),
                                               ConnectionPool(max_idle, cfg.pool.idle_timeout),
                                               cfg.ewma_tau);
    endpoint->admin_enabled = e.enabled;
    endpoint->upstream = name_;
    // Present at startup: no warm-up.  Added by a reload: warm up.
    if (built_) endpoint->warm_since = now;
    rebuilt.push_back(std::move(endpoint));
  }
  endpoints_ = std::move(rebuilt);
  balancer_->notify_membership(endpoints_);
  built_ = true;
}

std::pair<std::vector<EndpointPtr>, bool> Upstream::candidates(double now) const {
  std::vector<EndpointPtr> live;
  std::vector<EndpointPtr> configured;
  for (const EndpointPtr& e : endpoints_) {
    bool in = e->admin_enabled && !e->draining;
    bool available = in && e->selectable(now);
    if (available && !e->was_available) e->warm_since = now;   // back: start warming
    e->was_available = available;
    if (!in) continue;
    configured.push_back(e);
    if (available) live.push_back(e);
  }
  if (configured.empty()) return {{}, false};
  if (static_cast<double>(live.size()) / static_cast<double>(configured.size()) < panic_threshold_) {
    // Panic: trust nothing, spray everything that is administratively in.
    return {std::move(configured), true};
  }
  return {std::move(live), false};
}

EndpointPtr Upstream::select(const std::optional<std::string>& key, const std::vector<std::string>& exclude,
                             const std::string& preferred, double now) {
  maybe_detect_outliers(now);
  auto [pool, panic] = candidates(now);
  if (!exclude.empty()) {
    std::vector<EndpointPtr> filtered;
    for (const EndpointPtr& e : pool) {
      if (std::find(exclude.begin(), exclude.end(), e->name) == exclude.end()) filtered.push_back(e);
    }
    if (!filtered.empty()) pool = std::move(filtered);
  }
  if (!preferred.empty()) {
    for (const EndpointPtr& e : pool) {
      if (e->sticky_token() == preferred && (panic || e->breaker.allow(now))) return e;
    }
  }
  bool affinity = dynamic_cast<HashBalancer*>(balancer_.get()) != nullptr;
  bool declined = false;
  while (!pool.empty()) {
    // Once slow start has turned the policy's choice down, the replacement
    // comes from a stateless P2C.  Asking a stateful policy again (round
    // robin's counter, smooth WRR's running weights) correlates the retry
    // with the rejection and starves whichever endpoint sits next in line.
    EndpointPtr chosen = declined ? fallback_.pick(pool, key) : balancer_->pick(pool, key);
    if (!panic && !affinity && pool.size() > 1) {
      double share = chosen->slow_start_factor(now, slow_start_.window, slow_start_.min_weight);
      if (share < 1.0 && uniform(0.0, 1.0) >= share) {
        pool.erase(std::find(pool.begin(), pool.end(), chosen));
        declined = true;
        continue;
      }
    }
    // allow() only for the endpoint actually chosen: asking every candidate
    // would consume half-open probe slots that no request then uses.
    if (panic || chosen->breaker.allow(now)) return chosen;
    pool.erase(std::find(pool.begin(), pool.end(), chosen));
  }
  return nullptr;
}

size_t Upstream::usable(double now) const {
  return static_cast<size_t>(std::count_if(endpoints_.begin(), endpoints_.end(),
                                           [&](const EndpointPtr& e) { return e->selectable(now); }));
}

EndpointPtr Upstream::find(std::string_view name) const {
  for (const EndpointPtr& e : endpoints_) {
    if (e->name == name) return e;
  }
  return nullptr;
}

EndpointPtr Upstream::find_by_token(std::string_view token) const {
  for (const EndpointPtr& e : endpoints_) {
    if (e->sticky_token() == token) return e;
  }
  return nullptr;
}

void Upstream::maybe_detect_outliers(double now) {
  if (!outlier_.enabled || now < next_outlier_check_) return;
  detect_outliers(now);
}

void Upstream::detect_outliers(double now) {
  next_outlier_check_ = now + outlier_.interval;
  struct Sample {
    EndpointPtr endpoint;
    double success;
  };
  std::vector<Sample> samples;
  size_t configured = 0;
  size_t ejected = 0;
  for (const EndpointPtr& e : endpoints_) {
    uint64_t requests = e->total_requests - e->outlier_seen_requests;
    uint64_t failures = e->total_failures - e->outlier_seen_failures;
    e->outlier_seen_requests = e->total_requests;
    e->outlier_seen_failures = e->total_failures;
    if (!e->admin_enabled || e->draining) continue;
    ++configured;
    if (e->breaker.state() != BreakerState::Closed) {
      ++ejected;   // already out, by its breaker or a previous ejection
      continue;
    }
    if (requests < static_cast<uint64_t>(outlier_.min_requests)) continue;
    samples.push_back({e, 1.0 - static_cast<double>(failures) / static_cast<double>(requests)});
  }
  if (samples.size() < static_cast<size_t>(outlier_.min_hosts)) return;

  double sum = 0, squares = 0;
  for (const Sample& s : samples) {
    sum += s.success;
    squares += s.success * s.success;
  }
  double peers = static_cast<double>(samples.size() - 1);
  size_t cap = std::max<size_t>(1, static_cast<size_t>(outlier_.max_ejection_percent *
                                                       static_cast<double>(configured)));
  std::sort(samples.begin(), samples.end(),
            [](const Sample& a, const Sample& b) { return a.success < b.success; });
  for (const Sample& s : samples) {
    Endpoint& e = *s.endpoint;
    double mean = (sum - s.success) / peers;
    double variance = std::max(0.0, (squares - s.success * s.success) / peers - mean * mean);
    double gap = mean - s.success;
    bool outlier = gap > outlier_.stdev_factor * std::sqrt(variance) && gap > outlier_.min_gap;
    if (!outlier) {
      if (e.outlier_ejections > 0) --e.outlier_ejections;   // good interval: forgive one
      continue;
    }
    if (ejected >= cap) continue;
    ++ejected;
    ++e.outlier_ejections;
    double seconds = outlier_.base_ejection * std::min(e.outlier_ejections, 10);
    e.breaker.force_open(seconds, now);
    metrics::OUTLIER_EJECTIONS.inc({e.name});
    metrics::ENDPOINT_BREAKER.set({e.name}, static_cast<double>(e.breaker.state()));
    char detail[160];
    std::snprintf(detail, sizeof detail, "%.1f%% success vs %.1f%% for its peers; ejected for %gs",
                  s.success * 100.0, mean * 100.0, seconds);
    log::warning("outlier: " + e.name + " (" + name_ + ") " + detail);
  }
}

Json Upstream::snapshot(double now) {
  auto [_, panic] = candidates(now);
  Json out = Json::object();
  out["name"] = name_;
  out["policy"] = balancer_->name();
  out["panic_mode"] = panic;
  out["healthy"] = usable(now);
  out["total"] = endpoints_.size();
  out["sticky_cookie"] = spec_.sticky.cookie.empty() ? Json() : Json(spec_.sticky.cookie);
  Json names = Json::array();
  Json warming = Json::array();
  for (const EndpointPtr& e : endpoints_) {
    names.push_back(e->name);
    if (e->slow_start_factor(now, slow_start_.window, slow_start_.min_weight) < 1.0) warming.push_back(e->name);
  }
  out["endpoints"] = std::move(names);
  out["warming"] = std::move(warming);
  return out;
}

}  // namespace jdblance
