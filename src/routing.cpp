#include "routing.hpp"

#include <algorithm>

#include "metrics.hpp"

namespace jdblance {

bool route_host_matches(std::string_view pattern, std::string_view host) noexcept {
  if (pattern.empty()) return true;
  if (pattern.substr(0, 2) == "*.") {
    std::string_view suffix = pattern.substr(1);   // ".example.com"
    return host.size() > suffix.size() && host.substr(host.size() - suffix.size()) == suffix;
  }
  return host == pattern;
}

bool route_prefix_matches(std::string_view prefix, std::string_view path) noexcept {
  if (path.substr(0, prefix.size()) != prefix) return false;
  return path.size() == prefix.size() || prefix.back() == '/' || path[prefix.size()] == '/';
}

std::string request_host(const http::Request& req) {
  std::string host = http::to_lower(http::trim(req.get_or("host")));
  if (!host.empty() && host[0] == '[') {
    size_t close = host.find(']');
    return close == std::string::npos ? host : host.substr(0, close + 1);
  }
  size_t colon = host.find(':');
  return colon == std::string::npos ? host : host.substr(0, colon);
}

std::string strip_route_prefix(std::string_view target, std::string_view prefix) {
  if (prefix == "/" || target.substr(0, prefix.size()) != prefix) return std::string(target);
  std::string rest(target.substr(prefix.size()));
  if (prefix.back() == '/') rest.insert(rest.begin(), '/');   // "/api/" leaves "users" -> "/users"
  if (rest.empty() || rest[0] == '?') rest.insert(rest.begin(), '/');
  return rest;
}

// ---------------------------------------------------------------------------
// Cacheability
// ---------------------------------------------------------------------------

namespace {
struct Directives {
  bool no_store = false;
  bool no_cache = false;
  bool is_private = false;
  std::optional<double> max_age;
  std::optional<double> s_maxage;
};

std::optional<double> parse_seconds(std::string_view v) {
  if (v.size() >= 2 && v.front() == '"' && v.back() == '"') v = v.substr(1, v.size() - 2);
  if (v.empty() || v.size() > 10) return std::nullopt;
  double out = 0;
  for (char c : v) {
    if (c < '0' || c > '9') return std::nullopt;
    out = out * 10 + (c - '0');
  }
  return out;
}

Directives cache_control(const http::Message& msg) {
  Directives d;
  for (const std::string& line : msg.get_all("cache-control")) {
    std::string_view rest = line;
    while (!rest.empty()) {
      size_t comma = rest.find(',');
      std::string_view item = http::trim(rest.substr(0, comma));
      size_t eq = item.find('=');
      std::string name = http::to_lower(http::trim(item.substr(0, eq)));
      std::string_view value = eq == std::string_view::npos ? "" : http::trim(item.substr(eq + 1));
      if (name == "no-store") d.no_store = true;
      else if (name == "no-cache") d.no_cache = true;
      else if (name == "private") d.is_private = true;
      else if (name == "max-age") d.max_age = parse_seconds(value);
      else if (name == "s-maxage") d.s_maxage = parse_seconds(value);
      if (comma == std::string_view::npos) break;
      rest.remove_prefix(comma + 1);
    }
  }
  return d;
}
}  // namespace

bool request_may_use_cache(const http::Request& req, const http::Framing& framing) {
  if (req.method != "GET" && req.method != "HEAD") return false;
  if (!framing.empty() || req.has("authorization") || req.has("range")) return false;
  if (req.upgrade_target()) return false;
  Directives d = cache_control(req);
  return !d.no_store && !d.no_cache;
}

std::optional<Freshness> response_freshness(const http::Response& resp, const http::Framing& framing) {
  static const int storable[] = {200, 203, 204, 300, 301, 308, 404, 410};
  if (std::find(std::begin(storable), std::end(storable), resp.status) == std::end(storable)) {
    return std::nullopt;
  }
  if (resp.has("set-cookie") || resp.has("vary")) return std::nullopt;
  bool sized = framing.kind == http::BodyKind::Length ||
               (framing.kind == http::BodyKind::None && resp.status == 204);
  if (!sized) return std::nullopt;
  Directives d = cache_control(resp);
  if (d.no_store || d.no_cache || d.is_private) return std::nullopt;
  std::optional<double> lifetime = d.s_maxage ? d.s_maxage : d.max_age;
  if (!lifetime) return std::nullopt;
  double age = parse_seconds(resp.get_or("age")).value_or(0);
  if (*lifetime - age <= 0) return std::nullopt;
  return Freshness{*lifetime - age, age};
}

// ---------------------------------------------------------------------------
// ResponseCache
// ---------------------------------------------------------------------------

void ResponseCache::configure(size_t max_bytes, size_t max_object_bytes) {
  max_bytes_ = max_bytes;
  max_object_ = max_object_bytes;
  while (bytes_ > max_bytes_ && !lru_.empty()) {
    erase(std::prev(lru_.end()));
    metrics::CACHE.inc({"evict"});
  }
  publish();
}

size_t ResponseCache::cost(const Entry& entry) {
  size_t headers = 0;
  for (const auto& [k, v] : entry.second->headers) headers += k.size() + v.size() + 4;
  return entry.first.size() + entry.second->body.size() + headers + 128;
}

void ResponseCache::erase(std::list<Entry>::iterator it) {
  bytes_ -= cost(*it);
  index_.erase(it->first);
  lru_.erase(it);
}

void ResponseCache::publish() const {
  metrics::CACHE_BYTES.set(static_cast<double>(bytes_));
  metrics::CACHE_ENTRIES.set(static_cast<double>(index_.size()));
}

std::shared_ptr<const CachedResponse> ResponseCache::get(const std::string& key, double now) {
  auto found = index_.find(key);
  if (found == index_.end()) return nullptr;
  if (found->second->second->expires_at <= now) {
    erase(found->second);
    publish();
    return nullptr;
  }
  lru_.splice(lru_.begin(), lru_, found->second);
  return lru_.front().second;
}

void ResponseCache::put(const std::string& key, std::shared_ptr<const CachedResponse> response) {
  Entry entry{key, std::move(response)};
  size_t size = cost(entry);
  if (entry.second->body.size() > max_object_ || size > max_bytes_) return;
  if (auto found = index_.find(key); found != index_.end()) erase(found->second);
  lru_.push_front(std::move(entry));
  index_[key] = lru_.begin();
  bytes_ += size;
  metrics::CACHE.inc({"store"});
  while (bytes_ > max_bytes_) {
    erase(std::prev(lru_.end()));
    metrics::CACHE.inc({"evict"});
  }
  publish();
}

void ResponseCache::clear() {
  lru_.clear();
  index_.clear();
  bytes_ = 0;
  publish();
}

}  // namespace jdblance
