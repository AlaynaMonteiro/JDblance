// Request routing (host + path prefix -> upstream) and the response cache.
//
// The cache is deliberately conservative: it stores only what the origin
// explicitly marks as shareable, and serves it only to requests that could
// not have asked for anything else.
//
//   stored    status 200/203/204/300/301/308/404/410, Cache-Control s-maxage or
//             max-age > 0, no no-store / no-cache / private, no Set-Cookie,
//             no Vary, a Content-Length body up to cache.max_object_bytes
//   served    GET or HEAD with no body, no Authorization, no Range, and no
//             Cache-Control: no-cache / no-store from the client
//
// Without Vary support, a response that varies is never stored rather than
// served to the wrong client; Expires and revalidation are not implemented.
#pragma once

#include <list>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include "http1.hpp"

namespace jdblance {

// host: "" matches any; "example.com" exactly; "*.example.com" any subdomain.
bool route_host_matches(std::string_view pattern, std::string_view host) noexcept;
// Whole segments: "/api" matches "/api" and "/api/x", not "/apix".
bool route_prefix_matches(std::string_view prefix, std::string_view path) noexcept;
// The request's Host, lower-cased, without the port.
std::string request_host(const http::Request& req);
// "/api/users?id=1" minus "/api" -> "/users?id=1"; never returns an empty path.
std::string strip_route_prefix(std::string_view target, std::string_view prefix);

struct CachedResponse {
  int status = 200;
  std::string reason;
  http::Headers headers;   // end-to-end headers, minus Content-Length and Age
  std::string body;
  double stored_at = 0;
  double expires_at = 0;
  double initial_age = 0;  // Age the origin reported when it was stored
};

struct Freshness {
  double ttl;   // seconds left
  double age;   // what the origin already said in Age
};

bool request_may_use_cache(const http::Request& req, const http::Framing& framing);
// nullopt: must not be stored.
std::optional<Freshness> response_freshness(const http::Response& resp, const http::Framing& framing);

// LRU by bytes.  One per worker, like every other piece of datapath state.
class ResponseCache {
 public:
  void configure(size_t max_bytes, size_t max_object_bytes);
  size_t max_object_bytes() const noexcept { return max_object_; }
  // Fresh entry or null; expired entries are dropped on the way.
  std::shared_ptr<const CachedResponse> get(const std::string& key, double now);
  void put(const std::string& key, std::shared_ptr<const CachedResponse> response);
  size_t bytes() const noexcept { return bytes_; }
  size_t size() const noexcept { return index_.size(); }
  void clear();

 private:
  using Entry = std::pair<std::string, std::shared_ptr<const CachedResponse>>;
  static size_t cost(const Entry& entry);
  void erase(std::list<Entry>::iterator it);
  void publish() const;

  std::list<Entry> lru_;   // front = most recently used
  std::unordered_map<std::string, std::list<Entry>::iterator> index_;
  size_t bytes_ = 0;
  size_t max_bytes_ = 0;
  size_t max_object_ = 0;
};

}  // namespace jdblance
