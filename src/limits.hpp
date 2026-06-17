// Overload protection: rate limiting, retry budgets, adaptive concurrency, CoDel.
//
// Under stress a load balancer's real job is not "distribute" but "refuse".  A
// proxy that queues everything turns a 2x spike into a latency wall and then
// into an outage when clients retry.
//
//   TokenBucket / KeyedRateLimiter   per-client rate limits, bounded memory
//   RetryBudget                      retries as a *fraction* of traffic
//   GradientLimiter                  discovers backend capacity at runtime
//   AdmissionController              concurrency gate + CoDel queue that sheds
//                                    by sojourn time, not by depth, and goes
//                                    LIFO under overload
#pragma once

#include <deque>
#include <list>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include "config.hpp"
#include "json.hpp"
#include "loop.hpp"

namespace jdblance {

// Lazy token bucket: tokens are computed on read, no timer.
class TokenBucket {
 public:
  TokenBucket(double rate, std::optional<double> burst = std::nullopt);
  bool allow(double cost = 1.0, double now = monotonic());
  double tokens() const noexcept { return tokens_; }

 private:
  double rate_;
  double burst_;
  double tokens_;
  double stamp_;
};

// Per-key buckets with an LRU bound.  An unbounded map keyed by client IP is a
// memory-amplification bug waiting for a spoofed-source flood; evicting the
// least-recently-seen key is safe -- it comes back with a full bucket, which
// is what a genuinely idle client deserves.
class KeyedRateLimiter {
 public:
  KeyedRateLimiter(double rate, std::optional<double> burst = std::nullopt, size_t capacity = 65536);
  bool allow(const std::string& key, double cost = 1.0, double now = monotonic());
  size_t size() const noexcept { return order_.size(); }
  double rate() const noexcept { return rate_; }

 private:
  using Order = std::list<std::pair<std::string, TokenBucket>>;
  double rate_;
  std::optional<double> burst_;
  size_t capacity_;
  Order order_;   // front = most recent
  std::unordered_map<std::string, Order::iterator> index_;
};

// Fixed "3 retries per request" is how a partial outage becomes a total one:
// every client triples its load exactly when the backend is weakest.  A
// budget makes the retry rate proportional to live traffic, so retries taper
// off on their own when things are genuinely broken.
class RetryBudget {
 public:
  RetryBudget(double ratio = 0.1, double min_per_sec = 3.0, double window = 10.0);
  void configure(double ratio, double min_per_sec);
  void record_request(double now = monotonic());
  bool try_retry(double now = monotonic());
  Json snapshot(double now = monotonic());

 private:
  void trim(double now);
  double allowance() const;
  double ratio_;
  double min_per_sec_;
  double window_;
  std::deque<double> requests_;
  std::deque<double> retries_;
};

// Netflix-style gradient limiter.  Little's Law says concurrency =
// throughput x latency, so instead of hard-coding a limit (wrong the moment
// the backend moves to different hardware), estimate it:
//
//   gradient  = clamp(tolerance * rtt_noload / rtt_sample, 0.5, 1.0)
//   new_limit = limit * gradient + sqrt(limit)        <- tolerated queue
//   limit     = (1 - a) * limit + a * new_limit
class GradientLimiter {
 public:
  GradientLimiter(int initial = 32, int min_limit = 4, int max_limit = 4096, double smoothing = 0.2,
                  int probe_interval = 500, double backoff_ratio = 0.9, double rtt_tolerance = 1.5);
  void configure(int min_limit, int max_limit, double smoothing);
  void update(double rtt, int inflight, bool dropped = false);
  int current() const noexcept { return static_cast<int>(limit_); }
  double limit() const noexcept { return limit_; }
  Json snapshot() const;

 private:
  double limit_;
  int min_limit_;
  int max_limit_;
  double smoothing_;
  int probe_interval_;
  double backoff_ratio_;
  double rtt_tolerance_;
  double rtt_noload_ = 0.0;
  double sample_min_;
  int sample_count_ = 0;
  int samples_since_probe_ = 0;
};

// acquire() -> true: proceed; false: shed with 503.  Every true must be paired
// with exactly one release().
class AdmissionController {
 public:
  AdmissionController(std::unique_ptr<GradientLimiter> limiter, double target = 0.005,
                      double interval = 0.1, int max_queue = 1024, int static_limit = 0);
  static std::unique_ptr<AdmissionController> from_config(const ConcurrencyConfig& c);
  void configure(const ConcurrencyConfig& c);

  int limit() const noexcept;
  bool overloaded() const noexcept { return dropping_; }
  int inflight() const noexcept { return inflight_; }
  size_t queued() const noexcept { return queue_.size(); }
  GradientLimiter* limiter() noexcept { return limiter_.get(); }

  Task<bool> acquire();
  void release();
  Json snapshot() const;

 private:
  struct Waiter {
    Event ready;
    bool admitted = false;
    double enqueued = 0;
  };
  void promote();
  bool codel_should_drop(double sojourn, double now);

  std::unique_ptr<GradientLimiter> limiter_;
  int static_limit_;
  double target_;
  double interval_;
  int max_queue_;
  int inflight_ = 0;
  std::deque<std::shared_ptr<Waiter>> queue_;

  double first_above_ = 0.0;
  double drop_next_ = 0.0;
  int count_ = 0;
  bool dropping_ = false;
};

}  // namespace jdblance
