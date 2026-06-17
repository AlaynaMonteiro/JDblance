#include "metrics.hpp"

#include <cmath>
#include <limits>
#include <stdexcept>

#include "json.hpp"
#include "loop.hpp"

namespace jdblance::metrics {

namespace {
constexpr double kInf = std::numeric_limits<double>::infinity();

std::string escape(const std::string& s, bool quote) {
  std::string out;
  out.reserve(s.size());
  for (char c : s) {
    if (c == '\\') out += "\\\\";
    else if (c == '\n') out += "\\n";
    else if (quote && c == '"') out += "\\\"";
    else out.push_back(c);
  }
  return out;
}
}  // namespace

const std::vector<double> kDefaultBuckets = {0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1,
                                             0.25,   0.5,   1.0,    2.5,   5.0,  10.0,  kInf};

std::string format_value(double v) {
  if (v == kInf) return "+Inf";
  if (v == -kInf) return "-Inf";
  if (std::isnan(v)) return "NaN";
  return format_number(v);
}

Registry& registry() {
  static Registry r;
  return r;
}

std::string Registry::render() const {
  std::string out;
  for (const Metric* m : metrics_) m->expose(out);
  return out;
}

Metric::Metric(std::string name, std::string help, std::vector<std::string> label_names,
               Registry* reg)
    : name_(std::move(name)), help_(std::move(help)), label_names_(std::move(label_names)) {
  (reg ? *reg : registry()).add(this);
}

void Metric::check(const Labels& labels) const {
  if (labels.size() != label_names_.size()) {
    throw std::logic_error(name_ + ": expected " + std::to_string(label_names_.size()) +
                           " labels, got " + std::to_string(labels.size()));
  }
}

void Metric::header(std::string& out, const char* type) const {
  out += "# HELP " + name_ + " " + escape(help_, false) + "\n";
  out += "# TYPE " + name_ + " " + type + "\n";
}

std::string Metric::label_text(const Labels& values, const std::string& extra) const {
  if (values.empty() && extra.empty()) return "";
  std::string out = "{";
  for (size_t i = 0; i < values.size(); ++i) {
    if (i) out += ",";
    out += label_names_[i] + "=\"" + escape(values[i], true) + "\"";
  }
  if (!extra.empty()) {
    if (!values.empty()) out += ",";
    out += extra;
  }
  out += "}";
  return out;
}

// -- Counter -----------------------------------------------------------------

void Counter::inc(const Labels& labels, double amount) {
  check(labels);
  children_[labels] += amount;
}

double Counter::value(const Labels& labels) const {
  auto it = children_.find(labels);
  return it == children_.end() ? 0.0 : it->second;
}

double Counter::total() const {
  double sum = 0;
  for (const auto& [_, v] : children_) sum += v;
  return sum;
}

void Counter::expose(std::string& out) const {
  if (children_.empty()) return;
  header(out, "counter");
  for (const auto& [labels, v] : children_) {
    out += name_ + label_text(labels) + " " + format_value(v) + "\n";
  }
}

// -- Gauge -------------------------------------------------------------------

void Gauge::set(const Labels& labels, double v) {
  check(labels);
  children_[labels] = v;
}

void Gauge::inc(const Labels& labels, double amount) {
  check(labels);
  children_[labels] += amount;
}

double Gauge::value(const Labels& labels) const {
  auto it = children_.find(labels);
  return it == children_.end() ? 0.0 : it->second;
}

void Gauge::remove(const Labels& labels) { children_.erase(labels); }

void Gauge::expose(std::string& out) const {
  if (children_.empty()) return;
  header(out, "gauge");
  for (const auto& [labels, v] : children_) {
    out += name_ + label_text(labels) + " " + format_value(v) + "\n";
  }
}

// -- Histogram ---------------------------------------------------------------

Histogram::Histogram(std::string name, std::string help, std::vector<std::string> label_names,
                     std::vector<double> buckets, Registry* reg)
    : Metric(std::move(name), std::move(help), std::move(label_names), reg),
      buckets_(std::move(buckets)) {
  if (buckets_.empty() || buckets_.back() != kInf) buckets_.push_back(kInf);
}

void Histogram::observe(const Labels& labels, double v) {
  check(labels);
  Child& child = children_[labels];
  if (child.counts.empty()) child.counts.assign(buckets_.size(), 0);
  // Linear scan: 15 buckets, a predictable loop beats a binary search here.
  for (size_t i = 0; i < buckets_.size(); ++i) {
    if (v <= buckets_[i]) {
      ++child.counts[i];
      break;
    }
  }
  child.sum += v;
  ++child.count;
}

uint64_t Histogram::count(const Labels& labels) const {
  auto it = children_.find(labels);
  return it == children_.end() ? 0 : it->second.count;
}

std::optional<double> Histogram::quantile(double q, const Labels& labels) const {
  auto it = children_.find(labels);
  if (it == children_.end() || it->second.count < 2) return std::nullopt;
  const Child& child = it->second;
  double target = q * static_cast<double>(child.count);
  uint64_t cumulative = 0;
  double lower = 0.0;
  for (size_t i = 0; i < buckets_.size(); ++i) {
    uint64_t previous = cumulative;
    cumulative += child.counts[i];
    if (static_cast<double>(cumulative) >= target) {
      double bound = buckets_[i];
      if (bound == kInf) return lower;
      uint64_t span = child.counts[i];
      if (span == 0) return bound;
      double frac = (target - static_cast<double>(previous)) / static_cast<double>(span);
      return lower + (bound - lower) * frac;
    }
    if (buckets_[i] != kInf) lower = buckets_[i];
  }
  return lower;
}

void Histogram::expose(std::string& out) const {
  if (children_.empty()) return;
  header(out, "histogram");
  for (const auto& [labels, child] : children_) {
    uint64_t cumulative = 0;
    for (size_t i = 0; i < buckets_.size(); ++i) {
      cumulative += child.counts[i];
      out += name_ + "_bucket" + label_text(labels, "le=\"" + format_value(buckets_[i]) + "\"") +
             " " + std::to_string(cumulative) + "\n";
    }
    std::string base = label_text(labels);
    out += name_ + "_sum" + base + " " + format_value(child.sum) + "\n";
    out += name_ + "_count" + base + " " + std::to_string(child.count) + "\n";
  }
}

// ---------------------------------------------------------------------------

// Definition order is exposition order.
Gauge BUILD_INFO("jdblance_build_info", "Build metadata", {"version", "worker"});
Gauge START_TIME("jdblance_start_time_seconds", "Process start, unix epoch", {});
Counter REQUESTS("jdblance_requests_total", "Client requests by outcome",
                 {"method", "status", "endpoint"});
Histogram REQUEST_DURATION("jdblance_request_duration_seconds", "End-to-end client-observed latency",
                           {"endpoint"}, kDefaultBuckets);
Histogram UPSTREAM_DURATION("jdblance_upstream_duration_seconds", "Time to upstream response headers",
                            {"endpoint"}, kDefaultBuckets);
Histogram UPSTREAM_DURATION_ALL("jdblance_upstream_duration_all_seconds",
                                "Time to upstream response headers, all endpoints", {},
                                kDefaultBuckets);
Counter BYTES_IN("jdblance_client_bytes_received_total", "Request body bytes", {});
Counter BYTES_OUT("jdblance_client_bytes_sent_total", "Response body bytes", {});
Gauge ACTIVE_CONNS("jdblance_client_connections_active", "Open client connections", {});
Counter CONNS_TOTAL("jdblance_client_connections_total", "Accepted client connections", {});
Gauge INFLIGHT("jdblance_requests_in_flight", "Requests being proxied", {});
Gauge ENDPOINT_UP("jdblance_endpoint_up", "1 if endpoint passes health checks", {"endpoint"});
Gauge ENDPOINT_INFLIGHT("jdblance_endpoint_in_flight", "Requests in flight per endpoint",
                        {"endpoint"});
Gauge ENDPOINT_EWMA("jdblance_endpoint_ewma_seconds", "Latency EWMA used by the P2C balancer",
                    {"endpoint"});
Gauge ENDPOINT_BREAKER("jdblance_endpoint_breaker_state", "0 closed, 1 half-open, 2 open",
                       {"endpoint"});
Counter ENDPOINT_EJECTIONS("jdblance_endpoint_ejections_total", "Circuit breaker trips",
                           {"endpoint"});
Counter HEALTH_CHECKS("jdblance_health_checks_total", "Active health probes", {"endpoint", "result"});
Gauge POOL_IDLE("jdblance_pool_idle_connections", "Pooled idle upstream connections", {"endpoint"});
Counter POOL_REUSED("jdblance_pool_reused_total", "Upstream connections served from the pool",
                    {"endpoint"});
Counter POOL_CREATED("jdblance_pool_created_total", "Upstream connections dialled", {"endpoint"});
Counter RETRIES("jdblance_retries_total", "Retried attempts", {"reason"});
Counter RETRY_BUDGET_DENIED("jdblance_retry_budget_denied_total", "Retries refused by the budget", {});
Counter HEDGES("jdblance_hedges_total", "Hedged requests issued", {});
Counter HEDGE_WINS("jdblance_hedge_wins_total", "Hedges that beat the original", {});
Counter SHED("jdblance_shed_total", "Requests rejected before reaching an upstream", {"reason"});
Gauge QUEUE_DEPTH("jdblance_admission_queue_depth", "Requests waiting for an admission slot", {});
Histogram QUEUE_SOJOURN("jdblance_admission_sojourn_seconds", "Time spent in the admission queue", {},
                        {0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, kInf});
Gauge CONCURRENCY_LIMIT("jdblance_concurrency_limit", "Adaptive concurrency limit", {});
Gauge CONCURRENCY_RTT_NOLOAD("jdblance_concurrency_rtt_noload_seconds", "Estimated no-load RTT", {});
Counter UPGRADES("jdblance_protocol_upgrades_total", "101 Switching Protocols tunnels", {});
Counter TUNNEL_BYTES("jdblance_tunnel_bytes_total", "Bytes relayed inside upgraded tunnels",
                     {"direction"});
Counter CONFIG_RELOADS("jdblance_config_reloads_total", "SIGHUP reloads", {"result"});

namespace {
struct StartTime {
  StartTime() { START_TIME.set(wall_time()); }
} g_start_time;
}  // namespace

void forget_endpoint(const std::string& name) {
  Labels l{name};
  ENDPOINT_UP.remove(l);
  ENDPOINT_INFLIGHT.remove(l);
  ENDPOINT_EWMA.remove(l);
  ENDPOINT_BREAKER.remove(l);
  POOL_IDLE.remove(l);
}

}  // namespace jdblance::metrics
