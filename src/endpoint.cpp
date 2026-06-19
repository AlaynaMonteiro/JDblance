#include "endpoint.hpp"

#include <algorithm>
#include <cmath>

#include "hashring.hpp"
#include "metrics.hpp"

namespace jdblance {

// ---------------------------------------------------------------------------
// CircuitBreaker
// ---------------------------------------------------------------------------

CircuitBreaker::CircuitBreaker(double threshold, int min_requests, double window, double cooldown,
                               double max_cooldown, int consecutive_failures, int half_open_probes,
                               int buckets)
    : threshold_(threshold),
      min_requests_(min_requests),
      window_(window),
      cooldown_(cooldown),
      max_cooldown_(max_cooldown),
      consecutive_limit_(consecutive_failures),
      half_open_probes_(half_open_probes),
      buckets_(static_cast<size_t>(std::max(1, buckets))),
      bucket_span_(window / std::max(1, buckets)),
      bucket_start_(monotonic()) {}

CircuitBreaker::CircuitBreaker(const BreakerConfig& c)
    : CircuitBreaker(c.error_threshold, c.min_requests, c.window, c.cooldown, c.max_cooldown,
                     c.consecutive_failures, c.half_open_probes) {}

void CircuitBreaker::configure(const BreakerConfig& c) {
  threshold_ = c.error_threshold;
  min_requests_ = c.min_requests;
  cooldown_ = c.cooldown;
  max_cooldown_ = c.max_cooldown;
  consecutive_limit_ = c.consecutive_failures;
  half_open_probes_ = c.half_open_probes;
  if (c.window != window_) {
    window_ = c.window;
    bucket_span_ = window_ / static_cast<double>(buckets_.size());
    reset_counts();
    bucket_start_ = monotonic();
  }
}

void CircuitBreaker::roll(double now) {
  double elapsed = now - bucket_start_;
  if (elapsed < 0) {
    // Clock went backwards (or a test supplied its own): re-base rather than
    // roll the window backwards through every bucket.
    bucket_start_ = now;
    return;
  }
  if (elapsed < bucket_span_) return;
  double raw_steps = elapsed / bucket_span_;
  size_t steps = raw_steps >= static_cast<double>(buckets_.size())
                     ? buckets_.size()
                     : static_cast<size_t>(raw_steps);
  for (size_t i = 0; i < steps; ++i) {
    cursor_ = (cursor_ + 1) % buckets_.size();
    buckets_[cursor_] = Bucket{};
  }
  if (raw_steps >= static_cast<double>(buckets_.size())) {
    bucket_start_ = now;   // idle longer than the window: everything is stale
  } else {
    bucket_start_ += static_cast<double>(steps) * bucket_span_;
  }
}

bool CircuitBreaker::may_allow(double now) const {
  switch (state_) {
    case BreakerState::Closed: return true;
    case BreakerState::Open: return now >= open_until_;
    case BreakerState::HalfOpen:
      return probes_in_flight_ < half_open_probes_ || now >= probe_expires_;
  }
  return false;
}

bool CircuitBreaker::allow(double now) {
  roll(now);
  if (state_ == BreakerState::Open) {
    if (now < open_until_) return false;
    state_ = BreakerState::HalfOpen;
    probes_in_flight_ = 0;
  }
  if (state_ == BreakerState::HalfOpen) {
    if (probes_in_flight_ >= half_open_probes_) {
      if (now < probe_expires_) return false;
      probes_in_flight_ = 0;   // the earlier probes never reported back
    }
    ++probes_in_flight_;
    probe_expires_ = now + cooldown_;
  }
  return true;
}

bool CircuitBreaker::record(bool success, double now) {
  roll(now);
  Bucket& bucket = buckets_[cursor_];
  ++bucket.total;
  if (!success) {
    ++bucket.failed;
    ++consecutive_;
  } else {
    consecutive_ = 0;
  }

  if (state_ == BreakerState::HalfOpen) {
    probes_in_flight_ = std::max(0, probes_in_flight_ - 1);
    if (success) {
      force_close();
      return false;
    }
    trip(now);
    return true;
  }

  int total = 0, failed = 0;
  for (const Bucket& b : buckets_) {
    total += b.total;
    failed += b.failed;
  }
  bool rate_tripped = total >= min_requests_ &&
                      static_cast<double>(failed) / static_cast<double>(total) >= threshold_;
  bool streak_tripped = consecutive_ >= consecutive_limit_;
  if (state_ == BreakerState::Closed && (rate_tripped || streak_tripped)) {
    trip(now);
    return true;
  }
  return false;
}

void CircuitBreaker::trip(double now) {
  state_ = BreakerState::Open;
  ++trips_;
  double backoff = cooldown_ * std::pow(2.0, std::min(trips_ - 1, 30));
  open_until_ = now + std::min(backoff, max_cooldown_);
  probes_in_flight_ = 0;
}

void CircuitBreaker::reset_counts() {
  for (Bucket& b : buckets_) b = Bucket{};
}

void CircuitBreaker::force_open(double seconds, double now) {
  state_ = BreakerState::Open;
  open_until_ = now + seconds;
  probes_in_flight_ = 0;
}

void CircuitBreaker::force_close() {
  state_ = BreakerState::Closed;
  trips_ = 0;
  consecutive_ = 0;
  probes_in_flight_ = 0;
  reset_counts();
}

const char* CircuitBreaker::state_name() const noexcept {
  switch (state_) {
    case BreakerState::Closed: return "closed";
    case BreakerState::HalfOpen: return "half_open";
    case BreakerState::Open: return "open";
  }
  return "?";
}

Json CircuitBreaker::snapshot(double now) {
  // The window rolls lazily, so an idle endpoint would otherwise report the
  // counts from whenever its traffic stopped.
  roll(now);
  int total = 0, failed = 0;
  for (const Bucket& b : buckets_) {
    total += b.total;
    failed += b.failed;
  }
  Json out = Json::object();
  out["state"] = state_name();
  out["window_requests"] = total;
  out["window_failures"] = failed;
  out["consecutive_failures"] = consecutive_;
  out["trips"] = trips_;
  out["reopens_in"] = state_ == BreakerState::Open ? round_to(std::max(0.0, open_until_ - now), 3) : 0.0;
  return out;
}

// ---------------------------------------------------------------------------
// ConnectionPool
// ---------------------------------------------------------------------------

void ConnectionPool::configure(int max_idle, double idle_timeout) {
  max_idle_ = max_idle;
  idle_timeout_ = idle_timeout;
  while (static_cast<int>(idle_.size()) > max_idle_) {
    idle_.front().socket->close();
    idle_.pop_front();
  }
}

SocketPtr ConnectionPool::acquire() {
  double now = monotonic();
  while (!idle_.empty()) {
    Idle entry = std::move(idle_.back());
    idle_.pop_back();
    // MSG_PEEK tells us whether the origin closed it while it sat idle.
    if (now <= entry.expires && entry.socket->idle_and_open()) return entry.socket;
    entry.socket->close();
  }
  return nullptr;
}

bool ConnectionPool::release(SocketPtr socket) {
  if (!socket || socket->closed() || socket->cancelled() ||
      static_cast<int>(idle_.size()) >= max_idle_) {
    if (socket) socket->close();
    return false;
  }
  idle_.push_back(Idle{std::move(socket), monotonic() + idle_timeout_});
  return true;
}

size_t ConnectionPool::reap() {
  double now = monotonic();
  size_t removed = 0;
  for (auto it = idle_.begin(); it != idle_.end();) {
    if (now > it->expires || !it->socket->idle_and_open()) {
      it->socket->close();
      it = idle_.erase(it);
      ++removed;
    } else {
      ++it;
    }
  }
  return removed;
}

void ConnectionPool::clear() {
  for (Idle& entry : idle_) entry.socket->close();
  idle_.clear();
}

// ---------------------------------------------------------------------------
// Endpoint
// ---------------------------------------------------------------------------

namespace {
constexpr double kResolveTtl = 5.0;   // re-resolve names so a moved container is found
}

Endpoint::Endpoint(std::string name_, std::string host_, int port_, int weight_,
                   CircuitBreaker breaker_, ConnectionPool pool_, double ewma_tau_)
    : name(std::move(name_)),
      host(std::move(host_)),
      port(port_),
      weight(std::max(1, weight_)),
      breaker(std::move(breaker_)),
      pool(std::move(pool_)),
      ewma_tau(ewma_tau_),
      ewma_stamp_(monotonic()) {}

std::string Endpoint::address() const { return host + ":" + std::to_string(port); }

bool Endpoint::selectable(double now) const {
  return admin_enabled && !draining && healthy && breaker.may_allow(now);
}

double Endpoint::slow_start_factor(double now, double window, double min_weight) const noexcept {
  if (window <= 0 || now - warm_since >= window) return 1.0;
  return std::max(min_weight, (now - warm_since) / window);
}

std::string Endpoint::sticky_token() const {
  if (sticky_token_.empty()) {
    static const char* hex = "0123456789abcdef";
    uint64_t h = hash64(name, 0x5717);
    sticky_token_.assign(16, '0');
    for (int i = 15; i >= 0; --i, h >>= 4) sticky_token_[static_cast<size_t>(i)] = hex[h & 0xF];
  }
  return sticky_token_;
}

double Endpoint::ewma(double now) const {
  if (ewma_ == 0.0) return 0.0;
  return ewma_ * std::exp(-(now - ewma_stamp_) / ewma_tau);
}

void Endpoint::observe_latency(double seconds, double now) {
  if (ewma_ == 0.0) {
    ewma_ = seconds;
  } else {
    double decay = std::exp(-(now - ewma_stamp_) / ewma_tau);
    ewma_ = ewma_ * decay + seconds * (1.0 - decay);
  }
  ewma_stamp_ = now;
}

void Endpoint::observe_lower_bound(double seconds, double now) {
  if (seconds <= ewma(now)) return;
  ewma_ = seconds;
  ewma_stamp_ = now;
}

double Endpoint::load_score(double now, double default_latency) const {
  double latency;
  if (ewma_ == 0.0) {
    // Never measured.  A fixed default here starves the endpoint whenever
    // its peers are faster than the default: it would never be picked, so it
    // would never be measured.  Idle and unmeasured costs nothing -- it gets
    // one probe -- and while that probe is out it costs the default.
    if (inflight == 0) return 0.0;
    latency = default_latency;
  } else {
    latency = ewma(now);
  }
  return latency * (inflight + 1) / weight;
}

void Endpoint::add_inflight(int delta) { inflight += delta; }

void Endpoint::publish_gauges(double now) {
  metrics::ENDPOINT_INFLIGHT.set({name}, inflight);
  metrics::ENDPOINT_EWMA.set({name}, ewma(now));
  metrics::ENDPOINT_BREAKER.set({name}, static_cast<double>(breaker.state()));
  metrics::POOL_IDLE.set({name}, static_cast<double>(pool.size()));
}

Task<Endpoint::Conn> Endpoint::connect(double timeout) {
  if (SocketPtr pooled = pool.acquire()) {
    metrics::POOL_REUSED.inc({name});
    co_return Conn{std::move(pooled), true};
  }
  double deadline = monotonic() + timeout;
  if (addresses_.empty() || monotonic() - resolved_at_ > kResolveTtl) {
    std::vector<Address> fresh = co_await resolve(host, port, deadline);
    addresses_ = std::move(fresh);
    resolved_at_ = monotonic();
  }
  SocketPtr socket;
  try {
    socket = co_await connect_any(addresses_, deadline);
  } catch (const IoError&) {
    forget_addresses();   // the next attempt re-resolves
    throw;
  }
  metrics::POOL_CREATED.inc({name});
  co_return Conn{std::move(socket), false};
}

void Endpoint::release(SocketPtr socket, bool reusable) {
  if (reusable && !draining && admin_enabled) {
    pool.release(std::move(socket));
  } else if (socket) {
    socket->close();
  }
}

void Endpoint::record(bool success, int status, const std::string& error) {
  ++total_requests;
  if (!success) {
    ++total_failures;
    last_error = error.empty() && status ? "status " + std::to_string(status) : error;
  }
  last_status = status;
  if (breaker.record(success)) metrics::ENDPOINT_EJECTIONS.inc({name});
}

Json Endpoint::snapshot() {
  double now = monotonic();
  Json out = Json::object();
  out["name"] = name;
  out["upstream"] = upstream;
  out["address"] = address();
  out["weight"] = weight;
  out["healthy"] = healthy;
  out["enabled"] = admin_enabled;
  out["draining"] = draining;
  out["available"] = selectable(now);
  out["in_flight"] = inflight;
  out["idle_connections"] = pool.size();
  out["ewma_ms"] = round_to(ewma(now) * 1000.0, 3);
  out["load_score"] = round_to(load_score(now), 6);
  out["requests"] = total_requests;
  out["failures"] = total_failures;
  out["last_status"] = last_status ? Json(last_status) : Json();
  out["last_error"] = last_error.empty() ? Json() : Json(last_error);
  out["breaker"] = breaker.snapshot(now);
  return out;
}

}  // namespace jdblance
