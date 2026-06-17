// Load-balancing policies: (candidates, key) -> endpoint.
//
// Health, breaker and drain state are filtered out *before* a policy runs, so
// policies only decide "which of these good ones", never "is this one good".
//
//   round_robin            even request counts; ignores that requests differ
//   weighted_round_robin   nginx's smooth WRR: weighted but never bursty
//   random                 baseline; surprisingly hard to beat with 2 backends
//   least_conn             even concurrency
//   p2c_ewma               power of two choices over latency x queue (default)
//   consistent_hash        affinity, with bounded loads to cap hotspots
//   maglev                 affinity with O(1) lookup and minimal disruption
//
// Why p2c_ewma is the default: picking the globally least-loaded backend makes
// every worker pick the same "best" one at the same instant and herd onto it.
// Sampling two at random and taking the better one cuts maximum load from
// O(log n / log log n) to O(log log n) (Mitzenmacher) and has no herd.
#pragma once

#include <memory>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "endpoint.hpp"
#include "hashring.hpp"

namespace jdblance {

// Per-thread PRNG.  reseed_rng() must run in every forked worker: children
// inherit the parent's state, and N workers drawing the same "random" P2C
// pairs would herd exactly the way P2C exists to prevent.
std::mt19937_64& rng();
void reseed_rng();
double uniform(double lo, double hi);

class Balancer {
 public:
  virtual ~Balancer() = default;
  virtual const char* name() const noexcept = 0;
  // `candidates` is never empty.
  virtual EndpointPtr pick(const std::vector<EndpointPtr>& candidates,
                           const std::optional<std::string>& key) = 0;
  // Called when the configured endpoint set changes (not on health flaps).
  virtual void notify_membership(const std::vector<EndpointPtr>& /*all*/) {}
};

class RoundRobin final : public Balancer {
 public:
  const char* name() const noexcept override { return "round_robin"; }
  EndpointPtr pick(const std::vector<EndpointPtr>& c, const std::optional<std::string>&) override;

 private:
  uint64_t counter_ = 0;
};

class RandomChoice final : public Balancer {
 public:
  const char* name() const noexcept override { return "random"; }
  EndpointPtr pick(const std::vector<EndpointPtr>& c, const std::optional<std::string>&) override;
};

// Naive WRR emits AAABB for weights 3/2 -- a mini-burst.  Smooth WRR emits
// ABABA: add each weight to a running counter, take the max, subtract the
// total.  Same long-run ratio, no clustering.
class SmoothWeightedRoundRobin final : public Balancer {
 public:
  const char* name() const noexcept override { return "weighted_round_robin"; }
  EndpointPtr pick(const std::vector<EndpointPtr>& c, const std::optional<std::string>&) override;
};

class LeastConnections final : public Balancer {
 public:
  const char* name() const noexcept override { return "least_conn"; }
  EndpointPtr pick(const std::vector<EndpointPtr>& c, const std::optional<std::string>&) override;
};

class PeakEwmaP2C final : public Balancer {
 public:
  const char* name() const noexcept override { return "p2c_ewma"; }
  EndpointPtr pick(const std::vector<EndpointPtr>& c, const std::optional<std::string>&) override;
};

// Shared logic for the affinity policies.  The hash structure covers *every*
// configured endpoint, not just healthy ones, so a flapping backend does not
// reshuffle every other key.  When a key maps to an unavailable endpoint it is
// re-probed with a salt -- deterministic, so the same key always lands on the
// same backup -- and only then falls back to p2c.
class HashBalancer : public Balancer {
 public:
  EndpointPtr pick(const std::vector<EndpointPtr>& c, const std::optional<std::string>& key) override;
  void notify_membership(const std::vector<EndpointPtr>& all) override;

 protected:
  virtual std::optional<std::string> lookup(const std::string& key, int probe) = 0;
  virtual void rebuild(const std::vector<std::string>& members) = 0;
  static std::string probed(const std::string& key, int probe);

 private:
  PeakEwmaP2C fallback_;
  std::vector<std::string> signature_;
};

class ConsistentHash final : public HashBalancer {
 public:
  ConsistentHash(int vnodes = 160, double bounded_load_factor = 1.25)
      : vnodes_(vnodes), factor_(bounded_load_factor) {}
  const char* name() const noexcept override { return "consistent_hash"; }
  EndpointPtr pick(const std::vector<EndpointPtr>& c, const std::optional<std::string>& key) override;

 protected:
  std::optional<std::string> lookup(const std::string& key, int probe) override;
  void rebuild(const std::vector<std::string>& members) override;

 private:
  int vnodes_;
  double factor_;
  ConsistentHashRing ring_;
  const std::vector<EndpointPtr>* loads_ = nullptr;   // valid during pick()
  double total_load_ = 0;
};

class Maglev final : public HashBalancer {
 public:
  explicit Maglev(int table_size = 65537) : table_size_(table_size) {}
  const char* name() const noexcept override { return "maglev"; }
  std::map<std::string, int> distribution() const { return table_.distribution(); }

 protected:
  std::optional<std::string> lookup(const std::string& key, int probe) override;
  void rebuild(const std::vector<std::string>& members) override;

 private:
  int table_size_;
  MaglevTable table_;
};

// Throws std::invalid_argument for an unknown policy.
std::unique_ptr<Balancer> build_balancer(const std::string& policy, int vnodes = 160,
                                         double bounded_load_factor = 1.25,
                                         int maglev_table_size = 65537);

}  // namespace jdblance
