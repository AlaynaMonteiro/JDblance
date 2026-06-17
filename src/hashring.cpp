#include "hashring.hpp"

#include <algorithm>
#include <cstring>
#include <set>

namespace jdblance {

// ---------------------------------------------------------------------------
// BLAKE2b (RFC 7693), specialised: no key, 8-byte digest, salt in word 4.
// ---------------------------------------------------------------------------

namespace {
constexpr uint64_t kIV[8] = {
    0x6a09e667f3bcc908ULL, 0xbb67ae8584caa73bULL, 0x3c6ef372fe94f82bULL, 0xa54ff53a5f1d36f1ULL,
    0x510e527fade682d1ULL, 0x9b05688c2b3e6c1fULL, 0x1f83d9abfb41bd6bULL, 0x5be0cd19137e2179ULL,
};

constexpr uint8_t kSigma[12][16] = {
    {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
    {14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3},
    {11, 8, 12, 0, 5, 2, 15, 13, 10, 14, 3, 6, 7, 1, 9, 4},
    {7, 9, 3, 1, 13, 12, 11, 14, 2, 6, 5, 10, 4, 0, 15, 8},
    {9, 0, 5, 7, 2, 4, 10, 15, 14, 1, 11, 12, 6, 8, 3, 13},
    {2, 12, 6, 10, 0, 11, 8, 3, 4, 13, 7, 5, 15, 14, 1, 9},
    {12, 5, 1, 15, 14, 13, 4, 10, 0, 7, 6, 3, 9, 2, 8, 11},
    {13, 11, 7, 14, 12, 1, 3, 9, 5, 0, 15, 4, 8, 6, 2, 10},
    {6, 15, 14, 9, 11, 3, 0, 8, 12, 2, 13, 7, 1, 4, 10, 5},
    {10, 2, 8, 4, 7, 6, 1, 5, 15, 11, 9, 14, 3, 12, 13, 0},
    {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
    {14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3},
};

inline uint64_t rotr(uint64_t x, int n) { return (x >> n) | (x << (64 - n)); }

inline uint64_t load_le64(const uint8_t* p) {
  uint64_t v = 0;
  for (int i = 7; i >= 0; --i) v = (v << 8) | p[i];
  return v;
}

void compress(uint64_t h[8], const uint8_t block[128], uint64_t counter, bool last) {
  uint64_t m[16];
  for (int i = 0; i < 16; ++i) m[i] = load_le64(block + 8 * i);
  uint64_t v[16];
  for (int i = 0; i < 8; ++i) {
    v[i] = h[i];
    v[i + 8] = kIV[i];
  }
  v[12] ^= counter;   // low word of the 128-bit counter; inputs here are tiny
  if (last) v[14] = ~v[14];
  auto g = [&](int a, int b, int c, int d, uint64_t x, uint64_t y) {
    v[a] = v[a] + v[b] + x;
    v[d] = rotr(v[d] ^ v[a], 32);
    v[c] = v[c] + v[d];
    v[b] = rotr(v[b] ^ v[c], 24);
    v[a] = v[a] + v[b] + y;
    v[d] = rotr(v[d] ^ v[a], 16);
    v[c] = v[c] + v[d];
    v[b] = rotr(v[b] ^ v[c], 63);
  };
  for (int r = 0; r < 12; ++r) {
    const uint8_t* s = kSigma[r];
    g(0, 4, 8, 12, m[s[0]], m[s[1]]);
    g(1, 5, 9, 13, m[s[2]], m[s[3]]);
    g(2, 6, 10, 14, m[s[4]], m[s[5]]);
    g(3, 7, 11, 15, m[s[6]], m[s[7]]);
    g(0, 5, 10, 15, m[s[8]], m[s[9]]);
    g(1, 6, 11, 12, m[s[10]], m[s[11]]);
    g(2, 7, 8, 13, m[s[12]], m[s[13]]);
    g(3, 4, 9, 14, m[s[14]], m[s[15]]);
  }
  for (int i = 0; i < 8; ++i) h[i] ^= v[i] ^ v[i + 8];
}
}  // namespace

uint64_t hash64(std::string_view data, uint16_t seed) noexcept {
  uint64_t h[8];
  std::memcpy(h, kIV, sizeof h);
  h[0] ^= 0x01010000ULL ^ 8ULL;   // fanout 1, depth 1, no key, 8-byte digest
  h[4] ^= static_cast<uint64_t>(seed);   // salt bytes 0..7 (little-endian seed)

  const auto* p = reinterpret_cast<const uint8_t*>(data.data());
  size_t left = data.size();
  uint64_t counter = 0;
  uint8_t block[128];
  while (left > 128) {
    counter += 128;
    compress(h, p, counter, false);
    p += 128;
    left -= 128;
  }
  std::memset(block, 0, sizeof block);
  if (left) std::memcpy(block, p, left);
  counter += left;
  compress(h, block, counter, true);
  // digest = first 8 bytes of h[0], little-endian; read them as big-endian.
  return __builtin_bswap64(h[0]);
}

// ---------------------------------------------------------------------------
// Ring
// ---------------------------------------------------------------------------

ConsistentHashRing::ConsistentHashRing(std::vector<std::string> members, int vnodes)
    : members_(std::move(members)) {
  std::vector<std::pair<uint64_t, uint32_t>> pairs;
  pairs.reserve(members_.size() * static_cast<size_t>(vnodes));
  for (uint32_t m = 0; m < members_.size(); ++m) {
    for (int replica = 0; replica < vnodes; ++replica) {
      pairs.emplace_back(hash64(members_[m] + "#" + std::to_string(replica)), m);
    }
  }
  // Ties (vanishingly rare) break by member name, so the order is deterministic.
  std::sort(pairs.begin(), pairs.end(), [&](const auto& a, const auto& b) {
    if (a.first != b.first) return a.first < b.first;
    return members_[a.second] < members_[b.second];
  });
  for (const auto& [h, owner] : pairs) {
    hashes_.push_back(h);
    owners_.push_back(owner);
  }
}

size_t ConsistentHashRing::start_index(std::string_view key) const {
  uint64_t h = hash64(key);
  size_t idx = static_cast<size_t>(std::upper_bound(hashes_.begin(), hashes_.end(), h) - hashes_.begin());
  return idx % hashes_.size();
}

std::optional<std::string> ConsistentHashRing::get(std::string_view key) const {
  if (hashes_.empty()) return std::nullopt;
  return members_[owners_[start_index(key)]];
}

std::optional<std::string> ConsistentHashRing::get_bounded(
    std::string_view key, const std::function<double(const std::string&)>& load_of,
    double total_load, double factor) const {
  if (hashes_.empty()) return std::nullopt;
  double n = static_cast<double>(members_.size());
  double cap = std::max(1.0, factor * ((total_load + 1.0) / n));
  size_t start = start_index(key);
  std::vector<bool> seen(members_.size(), false);
  size_t distinct = 0;
  size_t idx = start;
  for (size_t step = 0; step < hashes_.size(); ++step) {
    uint32_t owner = owners_[idx];
    if (!seen[owner]) {
      seen[owner] = true;
      ++distinct;
      if (load_of(members_[owner]) < cap) return members_[owner];
      if (distinct == members_.size()) break;
    }
    idx = (idx + 1) % hashes_.size();
  }
  return members_[owners_[start]];
}

// ---------------------------------------------------------------------------
// Maglev
// ---------------------------------------------------------------------------

MaglevTable::MaglevTable(std::vector<std::string> members, uint32_t size)
    : members_(std::move(members)), size_(size) {
  size_t n = members_.size();
  if (n == 0 || size_ == 0) return;
  const uint64_t m = size_;
  // Per-member permutation of [0, m): `offset` picks the first slot and `skip`
  // is coprime with m (m is prime, 1 <= skip < m), so every slot is visited.
  std::vector<uint64_t> offset(n), skip(n), next(n, 0);
  for (size_t i = 0; i < n; ++i) {
    offset[i] = hash64(members_[i], 1) % m;
    skip[i] = hash64(members_[i], 2) % (m - 1) + 1;
  }
  constexpr uint32_t kEmpty = UINT32_MAX;
  table_.assign(m, kEmpty);
  uint64_t filled = 0;
  for (;;) {
    for (size_t i = 0; i < n; ++i) {
      uint64_t candidate = (offset[i] + next[i] * skip[i]) % m;
      while (table_[candidate] != kEmpty) {
        ++next[i];
        candidate = (offset[i] + next[i] * skip[i]) % m;
      }
      table_[candidate] = static_cast<uint32_t>(i);
      ++next[i];
      if (++filled == m) return;
    }
  }
}

std::optional<std::string> MaglevTable::get(std::string_view key) const {
  if (table_.empty()) return std::nullopt;
  return members_[table_[hash64(key) % size_]];
}

std::map<std::string, int> MaglevTable::distribution() const {
  std::map<std::string, int> counts;
  for (const auto& m : members_) counts[m] = 0;
  for (uint32_t idx : table_) ++counts[members_[idx]];
  return counts;
}

}  // namespace jdblance
