// One named pool of endpoints and the policy that chooses among them.
//
// Slow start and outlier detection live here because both act on selection:
//
//   * Slow start thins a warming endpoint's traffic by rejection sampling on
//     top of whatever the policy picked: accepted with probability
//     slow_start_factor, otherwise the policy picks again without it.  That
//     works with every policy without each one knowing about it.  Hash
//     policies are exempt, since diverting a key would break its affinity.
//   * Outlier detection runs lazily from select() once per interval: no
//     timer, and an upstream with no traffic has nothing to judge anyway.
//     Each endpoint is compared with its *peers* (leave-one-out mean and
//     stdev).  Including the endpoint in its own baseline, as Envoy does,
//     means that with fewer than five hosts even one failing every request
//     never falls below mean - 1.9 stdev.
#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "balancer.hpp"
#include "config.hpp"
#include "endpoint.hpp"
#include "json.hpp"

namespace jdblance {

class Upstream {
 public:
  explicit Upstream(std::string name) : name_(std::move(name)) {}

  // (Re)build from config.  `previous` is every endpoint of the old config:
  // one with the same name and address is carried over with its live state.
  void apply(const UpstreamConfig& spec, const Config& cfg, const std::vector<EndpointPtr>& previous);

  const std::string& name() const noexcept { return name_; }
  const UpstreamConfig& spec() const noexcept { return spec_; }
  const std::vector<EndpointPtr>& endpoints() const noexcept { return endpoints_; }
  Balancer& balancer() noexcept { return *balancer_; }

  // (candidates, panic).  Also notices endpoints that just became available,
  // which is when slow start begins.
  std::pair<std::vector<EndpointPtr>, bool> candidates(double now = monotonic()) const;
  // `preferred`: a sticky-cookie token; honoured when that endpoint is usable.
  EndpointPtr select(const std::optional<std::string>& key, const std::vector<std::string>& exclude,
                     const std::string& preferred = "", double now = monotonic());
  size_t usable(double now = monotonic()) const;
  bool in_panic(double now = monotonic()) const { return candidates(now).second; }
  EndpointPtr find(std::string_view name) const;
  EndpointPtr find_by_token(std::string_view token) const;

  // Exposed for tests: run outlier detection now, regardless of the interval.
  void detect_outliers(double now);
  Json snapshot(double now = monotonic());

 private:
  void maybe_detect_outliers(double now);

  std::string name_;
  UpstreamConfig spec_;
  double panic_threshold_ = 0.5;
  SlowStartConfig slow_start_;
  OutlierConfig outlier_;
  std::string balancer_signature_;
  std::unique_ptr<Balancer> balancer_;
  PeakEwmaP2C fallback_;   // re-picks after slow start declines the policy's choice
  std::vector<EndpointPtr> endpoints_;
  double next_outlier_check_ = 0;
  bool built_ = false;
};

}  // namespace jdblance
