#include "config.hpp"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <limits>
#include <set>
#include <sstream>

#include "http1.hpp"

namespace jdblance {

// ---------------------------------------------------------------------------
// Pre-processing
// ---------------------------------------------------------------------------

std::string strip_comments(std::string_view raw) {
  std::string out;
  out.reserve(raw.size());
  bool in_string = false;
  bool escaped = false;
  for (size_t i = 0; i < raw.size(); ++i) {
    char c = raw[i];
    if (in_string) {
      out.push_back(c);
      if (escaped) escaped = false;
      else if (c == '\\') escaped = true;
      else if (c == '"') in_string = false;
      continue;
    }
    if (c == '"') {
      in_string = true;
      out.push_back(c);
      continue;
    }
    if (c == '#' || (c == '/' && i + 1 < raw.size() && raw[i + 1] == '/')) {
      while (i < raw.size() && raw[i] != '\n') ++i;
      if (i < raw.size()) out.push_back('\n');   // keep line numbers in errors right
      continue;
    }
    out.push_back(c);
  }
  return out;
}

std::string expand_env(std::string_view raw) {
  auto ident_start = [](char c) { return c == '_' || std::isalpha(static_cast<unsigned char>(c)); };
  auto ident_char = [](char c) { return c == '_' || std::isalnum(static_cast<unsigned char>(c)); };
  std::string out;
  out.reserve(raw.size());
  size_t i = 0;
  while (i < raw.size()) {
    if (raw[i] == '$' && i + 2 < raw.size() && raw[i + 1] == '{' && ident_start(raw[i + 2])) {
      size_t j = i + 2;
      while (j < raw.size() && ident_char(raw[j])) ++j;
      std::string name(raw.substr(i + 2, j - i - 2));
      std::optional<std::string> fallback;
      size_t close = std::string_view::npos;
      if (j < raw.size() && raw[j] == '}') {
        close = j;
      } else if (raw.substr(j, 2) == ":-") {
        size_t end = raw.find('}', j + 2);
        if (end != std::string_view::npos) {
          fallback = std::string(raw.substr(j + 2, end - j - 2));
          close = end;
        }
      }
      if (close != std::string_view::npos) {
        const char* env = std::getenv(name.c_str());
        out += env ? std::string(env) : fallback.value_or("");
        i = close + 1;
        continue;
      }
    }
    out.push_back(raw[i++]);
  }
  return out;
}

// ---------------------------------------------------------------------------
// Visitors
// ---------------------------------------------------------------------------

namespace {

template <typename T, typename = void>
struct has_visit : std::false_type {};
template <typename T>
struct has_visit<T, std::void_t<decltype(std::declval<T&>().visit(std::declval<int&>()))>>
    : std::true_type {};

class Reader {
 public:
  Reader(const Json& data, std::string where) : where_(std::move(where)) {
    if (!data.is_object()) throw ConfigError(label() + "expected an object");
    obj_ = &data.as_object();
  }

  template <typename T>
  void operator()(const char* key, T& field) {
    seen_.insert(key);
    const Json* value = find(key);
    if (value == nullptr) return;
    read(*value, field, path(key));
  }

  void finish() const {
    std::vector<std::string> unknown;
    for (const auto& [k, _] : *obj_) {
      if (!seen_.count(k)) unknown.push_back(k);
    }
    if (unknown.empty()) return;
    std::string list;
    for (size_t i = 0; i < unknown.size(); ++i) list += (i ? ", " : "") + unknown[i];
    throw ConfigError(label() + (unknown.size() == 1 ? "unknown key " : "unknown keys ") + list);
  }

 private:
  std::string label() const { return where_.empty() ? "" : where_ + ": "; }
  std::string path(const char* key) const { return where_.empty() ? key : where_ + "." + key; }
  const Json* find(const std::string& key) const {
    for (const auto& [k, v] : *obj_) {
      if (k == key) return &v;
    }
    return nullptr;
  }

  [[noreturn]] static void mismatch(const std::string& where, const char* want, const Json& got) {
    throw ConfigError(where + ": expected " + want + ", got " + Json::type_name(got.type()));
  }

  static void read(const Json& v, bool& out, const std::string& where) {
    if (!v.is_bool()) mismatch(where, "a boolean", v);
    out = v.as_bool();
  }
  static void read(const Json& v, int& out, const std::string& where) {
    if (!v.is_number()) mismatch(where, "an integer", v);
    double d = v.as_number();
    if (d != std::floor(d) || d < std::numeric_limits<int>::min() ||
        d > std::numeric_limits<int>::max()) {
      throw ConfigError(where + ": expected an integer, got " + format_number(d));
    }
    out = static_cast<int>(d);
  }
  static void read(const Json& v, double& out, const std::string& where) {
    if (!v.is_number()) mismatch(where, "a number", v);
    out = v.as_number();
  }
  static void read(const Json& v, std::string& out, const std::string& where) {
    if (!v.is_string()) mismatch(where, "a string", v);
    out = v.as_string();
  }
  static void read(const Json& v, std::optional<std::string>& out, const std::string& where) {
    if (v.is_null()) {
      out.reset();
      return;
    }
    if (!v.is_string()) mismatch(where, "a string or null", v);
    out = v.as_string();
  }
  static void read(const Json& v, std::vector<EndpointConfig>& out, const std::string& where) {
    if (!v.is_array()) mismatch(where, "an array", v);
    out.clear();
    const auto& arr = v.as_array();
    for (size_t i = 0; i < arr.size(); ++i) {
      EndpointConfig e;
      read(arr[i], e, where + "[" + std::to_string(i) + "]");
      out.push_back(std::move(e));
    }
  }
  template <typename T, std::enable_if_t<has_visit<T>::value, int> = 0>
  static void read(const Json& v, T& out, const std::string& where) {
    Reader nested(v, where);
    out.visit(nested);
    nested.finish();
  }

  std::string where_;
  const Json::Object* obj_ = nullptr;
  std::set<std::string> seen_;
};

class Writer {
 public:
  Json out = Json::object();

  template <typename T>
  void operator()(const char* key, const T& field) {
    out[key] = write(field);
  }

 private:
  static Json write(bool v) { return Json(v); }
  static Json write(int v) { return Json(v); }
  static Json write(double v) { return Json(v); }
  static Json write(const std::string& v) { return Json(v); }
  static Json write(const std::optional<std::string>& v) { return v ? Json(*v) : Json(); }
  static Json write(const std::vector<EndpointConfig>& v) {
    Json arr = Json::array();
    for (const auto& e : v) arr.push_back(write(e));
    return arr;
  }
  template <typename T, std::enable_if_t<has_visit<T>::value, int> = 0>
  static Json write(const T& v) {
    Writer nested;
    const_cast<T&>(v).visit(nested);
    return std::move(nested.out);
  }
};

bool is_prime(int n) {
  if (n < 2) return false;
  for (int d = 2; static_cast<long long>(d) * d <= n; ++d) {
    if (n % d == 0) return false;
  }
  return true;
}

void require(bool ok, const std::string& message) {
  if (!ok) throw ConfigError(message);
}

bool parse_port(std::string_view s, int* port) {
  if (s.empty() || s.size() > 5) return false;
  int v = 0;
  for (char c : s) {
    if (c < '0' || c > '9') return false;
    v = v * 10 + (c - '0');
  }
  if (v < 1 || v > 65535) return false;
  *port = v;
  return true;
}

}  // namespace

bool valid_endpoint_name(std::string_view name) noexcept {
  // Names end up in URLs (/endpoints/<name>/drain), metric labels and logs.
  if (name.empty() || name.size() > 128) return false;
  for (unsigned char c : name) {
    if (!(std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~' || c == ':' || c == '@')) {
      return false;
    }
  }
  return true;
}

std::string EndpointConfig::host() const {
  size_t colon = address.rfind(':');
  return colon == std::string::npos ? address : address.substr(0, colon);
}

int EndpointConfig::port() const {
  size_t colon = address.rfind(':');
  int port = 0;
  if (colon == std::string::npos || !parse_port(std::string_view(address).substr(colon + 1), &port)) {
    return 0;
  }
  return port;
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

Config Config::load(const std::string& path, bool validate) {
  std::ifstream in(path, std::ios::binary);
  if (!in) throw ConfigError("cannot read " + path);
  std::stringstream buf;
  buf << in.rdbuf();
  Config cfg = from_text(buf.str(), validate);
  char* resolved = ::realpath(path.c_str(), nullptr);
  cfg.source_path = resolved ? resolved : path;
  std::free(resolved);
  return cfg;
}

Config Config::from_text(std::string_view text, bool validate) {
  std::string cleaned = expand_env(strip_comments(text));
  Json data;
  try {
    data = Json::parse(cleaned);
  } catch (const JsonError& e) {
    throw ConfigError(std::string("invalid JSON: ") + e.what());
  }
  return from_json(data, validate);
}

Config Config::from_json(const Json& data, bool validate) {
  Config cfg;
  Reader reader(data, "");
  cfg.visit(reader);
  reader.finish();
  if (validate) cfg.validate();
  return cfg;
}

Json Config::to_json() const {
  Writer w;
  const_cast<Config*>(this)->visit(w);
  return std::move(w.out);
}

void Config::validate() const {
  require(std::find(kPolicyNames.begin(), kPolicyNames.end(), policy) != kPolicyNames.end(),
          "unknown policy \"" + policy +
              "\"; valid: consistent_hash, least_conn, maglev, p2c_ewma, random, round_robin, "
              "weighted_round_robin");

  require(!endpoints.empty(), "at least one endpoint is required");
  std::set<std::string> names;
  for (size_t i = 0; i < endpoints.size(); ++i) {
    const EndpointConfig& e = endpoints[i];
    std::string label = e.name.empty() ? "endpoints[" + std::to_string(i) + "]" : "endpoint " + e.name;
    require(!e.name.empty(), label + ": name is required");
    require(valid_endpoint_name(e.name),
            label + ": name may only contain letters, digits and - _ . ~ : @");
    require(names.insert(e.name).second, "endpoint names must be unique (\"" + e.name + "\" repeats)");
    size_t colon = e.address.rfind(':');
    require(colon != std::string::npos && colon > 0, label + ": address must be host:port");
    int port = 0;
    require(parse_port(std::string_view(e.address).substr(colon + 1), &port),
            label + ": bad port in \"" + e.address + "\"");
    // Hostname, IPv4 or [IPv6]; nothing that could smuggle markup or
    // whitespace into the dashboard, logs or Host headers.
    std::string host = e.host();
    require(std::all_of(host.begin(), host.end(), [](char ch) {
              return std::isalnum(static_cast<unsigned char>(ch)) || ch == '.' || ch == '-' ||
                     ch == '_' || ch == ':' || ch == '[' || ch == ']' || ch == '%';
            }),
            label + ": invalid host in \"" + e.address + "\"");
    require(e.weight >= 1 && e.weight <= 1000000, label + ": weight must be between 1 and 1000000");
  }

  static const std::array<std::string_view, 5> sources = {"client_ip", "header", "cookie", "path",
                                                          "query"};
  require(std::find(sources.begin(), sources.end(), hash_key.source) != sources.end(),
          "unknown hash_key.source \"" + hash_key.source + "\"");
  require(!((hash_key.source == "header" || hash_key.source == "cookie" ||
             hash_key.source == "query") &&
            hash_key.name.empty()),
          "hash_key.source=" + hash_key.source + " requires hash_key.name");

  require(panic_threshold >= 0.0 && panic_threshold <= 1.0, "panic_threshold must be between 0 and 1");
  require(listen.workers >= 1 && listen.workers <= 256, "listen.workers must be between 1 and 256");
  require(listen.port >= 0 && listen.port <= 65535, "listen.port must be between 0 and 65535");
  require(listen.backlog >= 1, "listen.backlog must be >= 1");
  require(admin.port >= 0 && (admin.port == 0 || admin.port + listen.workers - 1 <= 65535),
          "admin.port (+ one port per worker) must fit in 0..65535");

  require(health.interval > 0 && health.timeout > 0, "health.interval and health.timeout must be > 0");
  require(health.rise >= 1 && health.fall >= 1, "health.rise and health.fall must be >= 1");
  require(!health.path.empty() && health.path[0] == '/', "health.path must start with /");
  require(health.expected_status >= 100 && health.expected_status <= 599,
          "health.expected_status must be an HTTP status");

  require(breaker.error_threshold > 0 && breaker.error_threshold <= 1,
          "breaker.error_threshold must be in (0, 1]");
  require(breaker.min_requests >= 1, "breaker.min_requests must be >= 1");
  require(breaker.window > 0, "breaker.window must be > 0");
  require(breaker.cooldown > 0 && breaker.max_cooldown >= breaker.cooldown,
          "breaker.cooldown must be > 0 and <= breaker.max_cooldown");
  require(breaker.consecutive_failures >= 1, "breaker.consecutive_failures must be >= 1");
  require(breaker.half_open_probes >= 1, "breaker.half_open_probes must be >= 1");

  require(retry.max_attempts >= 1 && retry.max_attempts <= 10, "retry.max_attempts must be 1..10");
  require(retry.budget_ratio >= 0 && retry.budget_min_per_sec >= 0, "retry budget must be >= 0");
  require(retry.buffer_bytes >= 0, "retry.buffer_bytes must be >= 0");
  require(retry.backoff_base >= 0 && retry.backoff_max >= 0, "retry backoff must be >= 0");

  require(hedge.quantile > 0 && hedge.quantile < 1, "hedge.quantile must be in (0, 1)");
  require(hedge.min_delay >= 0 && hedge.max_delay >= hedge.min_delay,
          "hedge.min_delay must be >= 0 and <= hedge.max_delay");

  require(limits.max_connections >= 1, "limits.max_connections must be >= 1");
  require(limits.rate_per_ip >= 0 && limits.rate_burst >= 0, "rate limits must be >= 0");
  require(limits.max_body_bytes >= 0, "limits.max_body_bytes must be >= 0");
  const ConcurrencyConfig& c = limits.concurrency;
  require(c.static_limit >= 0, "limits.concurrency.static_limit must be >= 0");
  require(c.min_limit >= 1 && c.max_limit >= c.min_limit,
          "limits.concurrency needs 1 <= min_limit <= max_limit");
  require(c.initial >= c.min_limit && c.initial <= c.max_limit,
          "limits.concurrency.initial must be between min_limit and max_limit");
  require(c.smoothing > 0 && c.smoothing <= 1, "limits.concurrency.smoothing must be in (0, 1]");
  require(c.max_queue >= 0, "limits.concurrency.max_queue must be >= 0");
  require(c.codel_target > 0 && c.codel_interval > 0, "CoDel target and interval must be > 0");

  require(timeouts.connect > 0 && timeouts.upstream_header > 0 && timeouts.upstream_body > 0 &&
              timeouts.client_idle > 0 && timeouts.client_header > 0 && timeouts.drain >= 0 &&
              timeouts.tunnel_idle > 0,
          "timeouts must be > 0");
  require(pool.max_idle_per_endpoint >= 0, "pool.max_idle_per_endpoint must be >= 0");
  require(pool.idle_timeout > 0, "pool.idle_timeout must be > 0");

  require(log.sample >= 0 && log.sample <= 1, "log.sample must be between 0 and 1");
  require(log.level == "debug" || log.level == "info" || log.level == "warning" ||
              log.level == "error",
          "log.level must be debug, info, warning or error");
  require(log.slow_request >= 0, "log.slow_request must be >= 0");

  require(ewma_tau > 0, "ewma_tau must be > 0");
  require(vnodes >= 1 && vnodes <= 10000, "vnodes must be 1..10000");
  require(bounded_load_factor >= 0, "bounded_load_factor must be >= 0 (0 disables the bound)");
  // Maglev's permutation only visits every slot when the size is prime.
  require(is_prime(maglev_table_size) && maglev_table_size <= 10000019 &&
              static_cast<size_t>(maglev_table_size) > endpoints.size(),
          "maglev_table_size must be a prime larger than the endpoint count (e.g. 65537)");
  // Goes out verbatim in every Via header.
  require(!server_name.empty() && server_name.size() <= 64 &&
              std::all_of(server_name.begin(), server_name.end(),
                          [](char ch) { return ch > 0x20 && ch < 0x7F; }) &&
              http::is_token(std::string_view(server_name).substr(0, server_name.find('/'))),
          "server_name must look like product/version");
}

}  // namespace jdblance
