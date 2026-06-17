#include "limits.hpp"

#include <algorithm>
#include <cmath>
#include <limits>

#include "metrics.hpp"

namespace jdblance {

// ---------------------------------------------------------------------------
// TokenBucket
// ---------------------------------------------------------------------------

TokenBucket::TokenBucket(double rate, std::optional<double> burst)
    : rate_(rate), burst_(burst ? *burst : std::max(rate, 1.0)), tokens_(burst_), stamp_(monotonic()) {}

bool TokenBucket::allow(double cost, double now) {
  // Clamp at zero: a rewound clock must never *remove* tokens.
  double elapsed = std::max(0.0, now - stamp_);
  tokens_ = std::min(burst_, tokens_ + elapsed * rate_);
  stamp_ = now;
  if (tokens_ >= cost) {
    tokens_ -= cost;
    return true;
  }
  return false;
}

KeyedRateLimiter::KeyedRateLimiter(double rate, std::optional<double> burst, size_t capacity)
    : rate_(rate), burst_(burst), capacity_(std::max<size_t>(1, capacity)) {}

bool KeyedRateLimiter::allow(const std::string& key, double cost, double now) {
  if (rate_ <= 0) return true;
  auto it = index_.find(key);
  if (it == index_.end()) {
    if (order_.size() >= capacity_) {
      index_.erase(order_.back().first);
      order_.pop_back();
    }
    order_.emplace_front(key, TokenBucket(rate_, burst_));
    it = index_.emplace(key, order_.begin()).first;
    // A fresh bucket's clock starts now, whatever clock the caller is using.
    order_.front().second.allow(0.0, now);
  } else {
    order_.splice(order_.begin(), order_, it->second);
  }
  return it->second->second.allow(cost, now);
}

// ---------------------------------------------------------------------------
// RetryBudget
// ---------------------------------------------------------------------------

RetryBudget::RetryBudget(double ratio, double min_per_sec, double window)
    : ratio_(ratio), min_per_sec_(min_per_sec), window_(window) {}

void RetryBudget::configure(double ratio, double min_per_sec) {
  ratio_ = ratio;
  min_per_sec_ = min_per_sec;
}

void RetryBudget::trim(double now) {
  double cutoff = now - window_;
  while (!requests_.empty() && requests_.front() < cutoff) requests_.pop_front();
  while (!retries_.empty() && retries_.front() < cutoff) retries_.pop_front();
}

double RetryBudget::allowance() const {
  return static_cast<double>(requests_.size()) * ratio_ + min_per_sec_ * window_;
}

void RetryBudget::record_request(double now) {
  requests_.push_back(now);
  trim(now);
}

bool RetryBudget::try_retry(double now) {
  trim(now);
  if (static_cast<double>(retries_.size()) < allowance()) {
    retries_.push_back(now);
    return true;
  }
  metrics::RETRY_BUDGET_DENIED.inc();
  return false;
}

Json RetryBudget::snapshot(double now) {
  trim(now);
  Json out = Json::object();
  out["window_requests"] = requests_.size();
  out["window_retries"] = retries_.size();
  out["allowance"] = round_to(allowance(), 2);
  return out;
}

// ---------------------------------------------------------------------------
// GradientLimiter
// ---------------------------------------------------------------------------

GradientLimiter::GradientLimiter(int initial, int min_limit, int max_limit, double smoothing,
                                 int probe_interval, double backoff_ratio, double rtt_tolerance)
    : limit_(initial),
      min_limit_(min_limit),
      max_limit_(max_limit),
      smoothing_(smoothing),
      probe_interval_(probe_interval),
      backoff_ratio_(backoff_ratio),
      rtt_tolerance_(rtt_tolerance),
      sample_min_(std::numeric_limits<double>::infinity()) {
  metrics::CONCURRENCY_LIMIT.set(limit_);
}

void GradientLimiter::configure(int min_limit, int max_limit, double smoothing) {
  min_limit_ = min_limit;
  max_limit_ = max_limit;
  smoothing_ = smoothing;
  limit_ = std::clamp(limit_, static_cast<double>(min_limit_), static_cast<double>(max_limit_));
  metrics::CONCURRENCY_LIMIT.set(limit_);
}

void GradientLimiter::update(double rtt, int /*inflight*/, bool dropped) {
  if (dropped) {
    // A drop is unambiguous evidence of overload: react now, not next window.
    limit_ = std::max(static_cast<double>(min_limit_), limit_ * backoff_ratio_);
    metrics::CONCURRENCY_LIMIT.set(limit_);
    return;
  }
  sample_min_ = std::min(sample_min_, rtt);
  ++sample_count_;
  ++samples_since_probe_;

  // One window per `limit` completions: enough samples to mean something,
  // short enough to track a backend that just got slower.
  if (sample_count_ < std::max(1, static_cast<int>(limit_))) return;

  double sample = sample_min_;
  sample_min_ = std::numeric_limits<double>::infinity();
  sample_count_ = 0;

  if (rtt_noload_ == 0.0) {
    rtt_noload_ = sample;
  } else if (samples_since_probe_ >= probe_interval_) {
    // Periodically forget the baseline, or a permanent improvement (a warmed
    // cache, a faster rollout) is never learned.
    samples_since_probe_ = 0;
    rtt_noload_ = sample;
  } else {
    rtt_noload_ = std::min(rtt_noload_, sample);
  }
  if (sample <= 0 || rtt_noload_ <= 0) return;

  double gradient = std::clamp(rtt_tolerance_ * rtt_noload_ / sample, 0.5, 1.0);
  double new_limit = limit_ * gradient + std::sqrt(limit_);
  // Never more than 2x per window: one fast sample must not throw the gate open.
  new_limit = std::min(new_limit, limit_ * 2);
  limit_ = std::clamp((1 - smoothing_) * limit_ + smoothing_ * new_limit,
                      static_cast<double>(min_limit_), static_cast<double>(max_limit_));
  metrics::CONCURRENCY_LIMIT.set(limit_);
  metrics::CONCURRENCY_RTT_NOLOAD.set(rtt_noload_);
}

Json GradientLimiter::snapshot() const {
  Json out = Json::object();
  out["limit"] = round_to(limit_, 2);
  out["rtt_noload_ms"] = round_to(rtt_noload_ * 1000.0, 3);
  return out;
}

// ---------------------------------------------------------------------------
// AdmissionController
// ---------------------------------------------------------------------------

AdmissionController::AdmissionController(std::unique_ptr<GradientLimiter> limiter, double target,
                                         double interval, int max_queue, int static_limit)
    : limiter_(std::move(limiter)),
      static_limit_(static_limit),
      target_(target),
      interval_(interval),
      max_queue_(max_queue) {}

std::unique_ptr<AdmissionController> AdmissionController::from_config(const ConcurrencyConfig& c) {
  std::unique_ptr<GradientLimiter> limiter;
  if (c.enabled && c.static_limit == 0) {
    limiter = std::make_unique<GradientLimiter>(c.initial, c.min_limit, c.max_limit, c.smoothing);
  }
  return std::make_unique<AdmissionController>(std::move(limiter), c.codel_target, c.codel_interval,
                                               c.max_queue, c.static_limit);
}

void AdmissionController::configure(const ConcurrencyConfig& c) {
  target_ = c.codel_target;
  interval_ = c.codel_interval;
  max_queue_ = c.max_queue;
  static_limit_ = c.static_limit;
  bool adaptive = c.enabled && c.static_limit == 0;
  if (adaptive && !limiter_) {
    limiter_ = std::make_unique<GradientLimiter>(c.initial, c.min_limit, c.max_limit, c.smoothing);
  } else if (!adaptive) {
    limiter_.reset();
  } else {
    limiter_->configure(c.min_limit, c.max_limit, c.smoothing);
  }
  promote();   // a larger limit may admit queued requests right away
}

int AdmissionController::limit() const noexcept {
  if (static_limit_ > 0) return static_limit_;
  return limiter_ ? limiter_->current() : (1 << 30);
}

Task<bool> AdmissionController::acquire() {
  if (inflight_ < limit() && queue_.empty()) {
    ++inflight_;
    co_return true;
  }
  if (static_cast<int>(queue_.size()) >= max_queue_) {
    metrics::SHED.inc({"queue_full"});
    co_return false;
  }
  auto waiter = std::make_shared<Waiter>();
  waiter->enqueued = monotonic();
  queue_.push_back(waiter);
  metrics::QUEUE_DEPTH.set(static_cast<double>(queue_.size()));
  co_await waiter->ready.wait();
  metrics::QUEUE_SOJOURN.observe(monotonic() - waiter->enqueued);
  if (!waiter->admitted) metrics::SHED.inc({"codel"});
  co_return waiter->admitted;
}

void AdmissionController::release() {
  --inflight_;
  promote();
}

void AdmissionController::promote() {
  double now = monotonic();
  while (!queue_.empty() && inflight_ < limit()) {
    // LIFO while overloaded: the newest request has the best chance that its
    // client is still waiting for the answer.
    std::shared_ptr<Waiter> waiter;
    if (dropping_) {
      waiter = std::move(queue_.back());
      queue_.pop_back();
    } else {
      waiter = std::move(queue_.front());
      queue_.pop_front();
    }
    if (codel_should_drop(now - waiter->enqueued, now)) {
      waiter->admitted = false;
      waiter->ready.set();
      continue;
    }
    ++inflight_;
    waiter->admitted = true;
    waiter->ready.set();
  }
  metrics::QUEUE_DEPTH.set(static_cast<double>(queue_.size()));
}

// The CoDel control law (Nichols/Jacobson), adapted to a request queue.
bool AdmissionController::codel_should_drop(double sojourn, double now) {
  if (sojourn < target_ || queue_.empty()) {
    // Draining fast enough: leave the dropping state.
    first_above_ = 0.0;
    dropping_ = false;
    return false;
  }
  if (first_above_ == 0.0) {
    // Start the clock; a brief burst above target is not congestion.
    first_above_ = now + interval_;
    return false;
  }
  if (now < first_above_) return false;
  if (!dropping_) {
    dropping_ = true;
    count_ = 1;
    drop_next_ = now + interval_;
    return true;
  }
  if (now < drop_next_) return false;
  ++count_;
  // Drop interval shrinks as 1/sqrt(count): persistent overload sheds
  // progressively harder instead of oscillating.
  drop_next_ = now + interval_ / std::sqrt(static_cast<double>(count_));
  return true;
}

Json AdmissionController::snapshot() const {
  Json out = Json::object();
  out["in_flight"] = inflight_;
  out["queued"] = queue_.size();
  out["limit"] = limit();
  out["codel_dropping"] = dropping_;
  if (limiter_) out["gradient"] = limiter_->snapshot();
  return out;
}

}  // namespace jdblance
