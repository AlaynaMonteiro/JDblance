// Per-backend state: circuit breaker, latency EWMA, connection pool.
//
// Everything the datapath knows about one origin lives here, so the balancer
// stays a pure selection function over a list of endpoints.
#pragma once

#include <deque>
#include <memory>
#include <string>
#include <vector>

#include "config.hpp"
#include "json.hpp"
#include "loop.hpp"

namespace jdblance {

enum class BreakerState : int { Closed = 0, HalfOpen = 1, Open = 2 };

// Rolling-window error-rate breaker with exponential backoff.
//
// Two independent trip conditions, because they catch different failures: the
// error *rate* over a window catches a degraded backend, *consecutive*
// failures catch one that just died without waiting for `min_requests`.
// Cooldown doubles per consecutive trip up to a cap; half-open admits
// `half_open_probes` at a time.  A probe that never reports back (its request
// was abandoned) expires after one cooldown instead of wedging the breaker
// half-open forever.
class CircuitBreaker {
 public:
  CircuitBreaker(double threshold = 0.5, int min_requests = 20, double window = 10.0,
                 double cooldown = 5.0, double max_cooldown = 60.0, int consecutive_failures = 5,
                 int half_open_probes = 1, int buckets = 10);
  explicit CircuitBreaker(const BreakerConfig& cfg);

  // Applies new thresholds, keeping the current state and counts.
  void configure(const BreakerConfig& cfg);

  // Consumes a half-open probe slot when it says yes.
  bool allow(double now = monotonic());
  // The same question without side effects: could allow() say yes right now?
  bool may_allow(double now = monotonic()) const;
  // Feeds one outcome back; true if this call tripped the breaker.
  bool record(bool success, double now = monotonic());

  void force_open(double seconds, double now = monotonic());
  void force_close();

  BreakerState state() const noexcept { return state_; }
  const char* state_name() const noexcept;
  Json snapshot(double now = monotonic());

 private:
  void roll(double now);
  void trip(double now);
  void reset_counts();

  double threshold_;
  int min_requests_;
  double window_;
  double cooldown_;
  double max_cooldown_;
  int consecutive_limit_;
  int half_open_probes_;

  struct Bucket {
    int total = 0;
    int failed = 0;
  };
  std::vector<Bucket> buckets_;
  double bucket_span_;
  double bucket_start_;
  size_t cursor_ = 0;

  BreakerState state_ = BreakerState::Closed;
  double open_until_ = 0.0;
  double probe_expires_ = 0.0;
  int trips_ = 0;
  int consecutive_ = 0;
  int probes_in_flight_ = 0;
};

// Keep-alive pool for one endpoint.  LIFO: the most recently used socket is
// the likeliest to still be open on the origin side and has the warmest
// congestion window; FIFO would cycle every socket to the origin's idle
// timeout and maximise resets.
class ConnectionPool {
 public:
  ConnectionPool(int max_idle = 32, double idle_timeout = 30.0)
      : max_idle_(max_idle), idle_timeout_(idle_timeout) {}
  void configure(int max_idle, double idle_timeout);

  size_t size() const noexcept { return idle_.size(); }
  SocketPtr acquire();
  bool release(SocketPtr socket);   // false if it was closed instead
  size_t reap();
  void clear();

 private:
  struct Idle {
    SocketPtr socket;
    double expires;
  };
  int max_idle_;
  double idle_timeout_;
  std::deque<Idle> idle_;
};

class Endpoint {
 public:
  Endpoint(std::string name, std::string host, int port, int weight = 1,
           CircuitBreaker breaker = CircuitBreaker(), ConnectionPool pool = ConnectionPool(),
           double ewma_tau = 10.0);

  std::string name;
  std::string host;
  int port;
  int weight;
  CircuitBreaker breaker;
  ConnectionPool pool;
  double ewma_tau;

  bool healthy = true;          // active health check verdict
  bool admin_enabled = true;    // operator switch
  bool draining = false;
  int inflight = 0;
  int rise_count = 0;
  int fall_count = 0;
  long long smooth_weight = 0;  // smooth-WRR running weight
  uint64_t total_requests = 0;
  uint64_t total_failures = 0;
  std::string last_error;       // empty = none
  int last_status = 0;          // 0 = none
  std::string upstream = kDefaultUpstream;

  // Slow start: when the endpoint last became available after not being.
  // -inf means it never needs warming (present since startup).
  bool was_available = true;
  double warm_since = -kForever;
  // Outlier detection: counters at the last evaluation, and how many times
  // in a row it has been ejected (each ejection lasts longer).
  uint64_t outlier_seen_requests = 0;
  uint64_t outlier_seen_failures = 0;
  int outlier_ejections = 0;

  std::string address() const;
  // Admin- and health-wise usable, and the breaker would admit a request.
  bool selectable(double now = monotonic()) const;
  // Share of normal traffic while warming up: min_weight rising linearly to 1
  // over `window` seconds after warm_since.  1 when not warming.
  double slow_start_factor(double now, double window, double min_weight) const noexcept;
  // Opaque cookie value naming this endpoint (no backend names leak to clients).
  std::string sticky_token() const;

  // Time-decayed latency estimate.  Decays toward zero while idle, so a
  // backend that was slow an hour ago is not punished forever: an idle
  // endpoint looks attractive, and if it is still slow the next sample says so.
  double ewma(double now = monotonic()) const;
  void observe_latency(double seconds, double now = monotonic());
  // A censored sample: the request took *at least* this long (a hedge loser,
  // cancelled while waiting).  Raises the estimate, never lowers it.
  void observe_lower_bound(double seconds, double now = monotonic());
  // Finagle's peak-EWMA cost: latency * (queue depth + 1) / weight.  The
  // in-flight multiplier is what keeps it stable: a fast backend that was just
  // handed 50 requests looks expensive until it drains them.  An endpoint
  // with no measurement yet costs 0 while idle, so it is always probed.
  double load_score(double now = monotonic(), double default_latency = 0.005) const;

  void add_inflight(int delta);

  struct Conn {
    SocketPtr socket;
    bool reused;
  };
  Task<Conn> connect(double timeout);
  void release(SocketPtr socket, bool reusable);
  void record(bool success, int status = 0, const std::string& error = "");
  void forget_addresses() noexcept { resolved_at_ = -kForever; }

  Json snapshot();

 private:
  mutable std::string sticky_token_;
  double ewma_ = 0.0;
  double ewma_stamp_;
  std::vector<Address> addresses_;
  double resolved_at_ = -kForever;
};

using EndpointPtr = std::shared_ptr<Endpoint>;

}  // namespace jdblance
