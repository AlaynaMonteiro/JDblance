#include "balancer.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>

namespace jdblance {

std::mt19937_64& rng() {
  thread_local std::mt19937_64 engine{std::random_device{}()};
  return engine;
}

void reseed_rng() {
  std::random_device rd;
  std::seed_seq seq{rd(), rd(), rd(), rd()};
  rng().seed(seq);
}

double uniform(double lo, double hi) { return std::uniform_real_distribution<double>(lo, hi)(rng()); }

namespace {
size_t random_index(size_t n) { return std::uniform_int_distribution<size_t>(0, n - 1)(rng()); }
constexpr int kMaxHashProbes = 8;
}  // namespace

EndpointPtr RoundRobin::pick(const std::vector<EndpointPtr>& c, const std::optional<std::string>&) {
  return c[counter_++ % c.size()];
}

EndpointPtr RandomChoice::pick(const std::vector<EndpointPtr>& c, const std::optional<std::string>&) {
  return c[random_index(c.size())];
}

EndpointPtr SmoothWeightedRoundRobin::pick(const std::vector<EndpointPtr>& c,
                                           const std::optional<std::string>&) {
  long long total = 0;
  EndpointPtr best;
  for (const auto& e : c) {
    e->smooth_weight += e->weight;
    total += e->weight;
    if (!best || e->smooth_weight > best->smooth_weight) best = e;
  }
  best->smooth_weight -= total;
  return best;
}

EndpointPtr LeastConnections::pick(const std::vector<EndpointPtr>& c,
                                   const std::optional<std::string>&) {
  EndpointPtr best;
  double best_score = std::numeric_limits<double>::infinity();
  size_t ties = 0;
  for (const auto& e : c) {
    double score = static_cast<double>(e->inflight) / e->weight;
    if (score < best_score) {
      best = e;
      best_score = score;
      ties = 1;
    } else if (score == best_score && random_index(++ties) == 0) {
      best = e;   // reservoir sampling: ties resolve uniformly
    }
  }
  return best;
}

EndpointPtr PeakEwmaP2C::pick(const std::vector<EndpointPtr>& c, const std::optional<std::string>&) {
  size_t n = c.size();
  if (n == 1) return c[0];
  size_t i = random_index(n);
  size_t j = random_index(n - 1);
  if (j >= i) ++j;   // sample without replacement, no rejection loop
  double now = monotonic();
  return c[i]->load_score(now) <= c[j]->load_score(now) ? c[i] : c[j];
}

// -- hashing -----------------------------------------------------------------

std::string HashBalancer::probed(const std::string& key, int probe) {
  return probe == 0 ? key : key + "|" + std::to_string(probe);
}

void HashBalancer::notify_membership(const std::vector<EndpointPtr>& all) {
  std::vector<std::string> signature;
  signature.reserve(all.size());
  for (const auto& e : all) signature.push_back(e->name);
  std::sort(signature.begin(), signature.end());
  if (signature != signature_) {
    signature_ = std::move(signature);
    rebuild(signature_);
  }
}

EndpointPtr HashBalancer::pick(const std::vector<EndpointPtr>& c, const std::optional<std::string>& key) {
  if (!key) return fallback_.pick(c, key);
  if (signature_.empty()) notify_membership(c);
  for (int probe = 0; probe < kMaxHashProbes; ++probe) {
    std::optional<std::string> name = lookup(*key, probe);
    if (!name) break;
    for (const auto& e : c) {
      if (e->name == *name) return e;
    }
  }
  return fallback_.pick(c, key);
}

EndpointPtr ConsistentHash::pick(const std::vector<EndpointPtr>& c,
                                 const std::optional<std::string>& key) {
  // Bounded load needs live in-flight counts; they are read from the
  // candidates for the duration of this call, so the ring stays pure data.
  loads_ = &c;
  total_load_ = 0;
  for (const auto& e : c) total_load_ += e->inflight;
  EndpointPtr chosen = HashBalancer::pick(c, key);
  loads_ = nullptr;
  return chosen;
}

std::optional<std::string> ConsistentHash::lookup(const std::string& key, int probe) {
  std::string k = probed(key, probe);
  if (factor_ > 0 && loads_ != nullptr) {
    const auto* loads = loads_;
    auto load_of = [loads](const std::string& name) {
      for (const auto& e : *loads) {
        if (e->name == name) return static_cast<double>(e->inflight);
      }
      return 0.0;
    };
    return ring_.get_bounded(k, load_of, total_load_, factor_);
  }
  return ring_.get(k);
}

void ConsistentHash::rebuild(const std::vector<std::string>& members) {
  ring_ = ConsistentHashRing(members, vnodes_);
}

std::optional<std::string> Maglev::lookup(const std::string& key, int probe) {
  return table_.get(probed(key, probe));
}

void Maglev::rebuild(const std::vector<std::string>& members) {
  table_ = MaglevTable(members, static_cast<uint32_t>(table_size_));
}

std::unique_ptr<Balancer> build_balancer(const std::string& policy, int vnodes,
                                         double bounded_load_factor, int maglev_table_size) {
  if (policy == "round_robin") return std::make_unique<RoundRobin>();
  if (policy == "weighted_round_robin") return std::make_unique<SmoothWeightedRoundRobin>();
  if (policy == "random") return std::make_unique<RandomChoice>();
  if (policy == "least_conn") return std::make_unique<LeastConnections>();
  if (policy == "p2c_ewma") return std::make_unique<PeakEwmaP2C>();
  if (policy == "consistent_hash") return std::make_unique<ConsistentHash>(vnodes, bounded_load_factor);
  if (policy == "maglev") return std::make_unique<Maglev>(maglev_table_size);
  throw std::invalid_argument("unknown policy \"" + policy + "\"");
}

}  // namespace jdblance
