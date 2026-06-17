// Stable hashing schemes for session affinity.
//
// ConsistentHashRing: Karger ring with virtual nodes, plus Consistent Hashing
//   with Bounded Loads (Mirrokni/Thorup/Zadimoghaddam, 2016): the walk skips
//   any member already above `c * average` in-flight, so one hot key cannot
//   pin one backend.
// MaglevTable: Google's Maglev lookup table (NSDI '16).  O(1) lookup -- one
//   modulo, one index -- and removing one of N members moves ~1/N of keys.
//
// Both hash with BLAKE2b-64: stable across processes, builds and machines, so
// every worker (and every restart) puts a key on the same backend.
#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace jdblance {

// BLAKE2b with an 8-byte digest and salt = little-endian `seed`; the digest
// bytes are read as a big-endian integer.
uint64_t hash64(std::string_view data, uint16_t seed = 0) noexcept;

class ConsistentHashRing {
 public:
  ConsistentHashRing() = default;
  ConsistentHashRing(std::vector<std::string> members, int vnodes);

  size_t size() const noexcept { return members_.size(); }
  std::optional<std::string> get(std::string_view key) const;
  // Ring walk that skips members at or above `factor * average` load.  If all
  // are over the cap, returns the first choice: the bound spreads hotspots,
  // it does not refuse service.
  std::optional<std::string> get_bounded(std::string_view key,
                                         const std::function<double(const std::string&)>& load_of,
                                         double total_load, double factor) const;

 private:
  size_t start_index(std::string_view key) const;
  std::vector<std::string> members_;
  std::vector<uint64_t> hashes_;
  std::vector<uint32_t> owners_;   // index into members_
};

class MaglevTable {
 public:
  MaglevTable() = default;
  MaglevTable(std::vector<std::string> members, uint32_t size);

  std::optional<std::string> get(std::string_view key) const;
  std::map<std::string, int> distribution() const;

 private:
  std::vector<std::string> members_;
  uint32_t size_ = 0;
  std::vector<uint32_t> table_;
};

}  // namespace jdblance
