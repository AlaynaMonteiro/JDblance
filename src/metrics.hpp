// Prometheus registry, text exposition format 0.0.4.
//
// Metrics are process-global, one registry per worker process.  Histograms
// double as latency estimators: `quantile()` interpolates inside the bucket
// holding the requested rank, which is how hedging gets a p99 without keeping
// samples.
#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace jdblance::metrics {

using Labels = std::vector<std::string>;

class Registry;

class Metric {
 public:
  // Registers itself with `reg` (the process registry by default).
  Metric(std::string name, std::string help, std::vector<std::string> label_names,
         Registry* reg = nullptr);
  virtual ~Metric() = default;
  Metric(const Metric&) = delete;
  Metric& operator=(const Metric&) = delete;

  const std::string& name() const noexcept { return name_; }
  virtual void expose(std::string& out) const = 0;

 protected:
  void check(const Labels& labels) const;
  void header(std::string& out, const char* type) const;
  std::string label_text(const Labels& values, const std::string& extra = "") const;

  std::string name_;
  std::string help_;
  std::vector<std::string> label_names_;
};

class Counter : public Metric {
 public:
  using Metric::Metric;
  void inc(double amount = 1.0) { inc(Labels{}, amount); }
  void inc(const Labels& labels, double amount = 1.0);
  double value(const Labels& labels = {}) const;
  double total() const;
  const std::map<Labels, double>& children() const noexcept { return children_; }
  void expose(std::string& out) const override;

 private:
  std::map<Labels, double> children_;
};

class Gauge : public Metric {
 public:
  using Metric::Metric;
  void set(double v) { set(Labels{}, v); }
  void set(const Labels& labels, double v);
  void inc(const Labels& labels, double amount = 1.0);
  double value(const Labels& labels = {}) const;
  void remove(const Labels& labels);
  void expose(std::string& out) const override;

 private:
  std::map<Labels, double> children_;
};

class Histogram : public Metric {
 public:
  Histogram(std::string name, std::string help, std::vector<std::string> label_names,
            std::vector<double> buckets, Registry* reg = nullptr);
  void observe(double v) { observe(Labels{}, v); }
  void observe(const Labels& labels, double v);
  // Interpolated quantile, or nullopt with fewer than 2 samples.  A rank in
  // the +Inf bucket returns the last finite bound: an intentional
  // under-estimate, since hedging on a huge bogus value would disable it.
  std::optional<double> quantile(double q, const Labels& labels = {}) const;
  uint64_t count(const Labels& labels = {}) const;
  void expose(std::string& out) const override;

 private:
  struct Child {
    std::vector<uint64_t> counts;
    double sum = 0;
    uint64_t count = 0;
  };
  std::vector<double> buckets_;
  std::map<Labels, Child> children_;
};

class Registry {
 public:
  void add(Metric* m) { metrics_.push_back(m); }
  std::string render() const;

 private:
  std::vector<Metric*> metrics_;
};

Registry& registry();
std::string format_value(double v);
extern const std::vector<double> kDefaultBuckets;

// ---------------------------------------------------------------------------
// The load balancer's metric surface (names are the dashboard's contract).
// ---------------------------------------------------------------------------

extern Gauge BUILD_INFO;
extern Gauge START_TIME;
extern Counter REQUESTS;
extern Histogram REQUEST_DURATION;
extern Histogram UPSTREAM_DURATION;
// Unlabelled twin of UPSTREAM_DURATION: hedging needs the *cluster* p99, not
// each endpoint's own tail.
extern Histogram UPSTREAM_DURATION_ALL;
extern Counter BYTES_IN;
extern Counter BYTES_OUT;
extern Gauge ACTIVE_CONNS;
extern Counter CONNS_TOTAL;
extern Gauge INFLIGHT;
extern Gauge ENDPOINT_UP;
extern Gauge ENDPOINT_INFLIGHT;
extern Gauge ENDPOINT_EWMA;
extern Gauge ENDPOINT_BREAKER;
extern Counter ENDPOINT_EJECTIONS;
extern Counter HEALTH_CHECKS;
extern Gauge POOL_IDLE;
extern Counter POOL_REUSED;
extern Counter POOL_CREATED;
extern Counter RETRIES;
extern Counter RETRY_BUDGET_DENIED;
extern Counter HEDGES;
extern Counter HEDGE_WINS;
extern Counter SHED;
extern Gauge QUEUE_DEPTH;
extern Histogram QUEUE_SOJOURN;
extern Gauge CONCURRENCY_LIMIT;
extern Gauge CONCURRENCY_RTT_NOLOAD;
extern Counter UPGRADES;
extern Counter TUNNEL_BYTES;
extern Counter CONFIG_RELOADS;
extern Counter ROUTE_REQUESTS;
extern Counter OUTLIER_EJECTIONS;
extern Counter CACHE;   // result = hit | miss | store | evict
extern Gauge CACHE_BYTES;
extern Gauge CACHE_ENTRIES;

// Drop every per-endpoint gauge for an endpoint that left the config.
void forget_endpoint(const std::string& name);

}  // namespace jdblance::metrics
