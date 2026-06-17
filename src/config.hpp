// Configuration: JSON with `//` and `#` comments and ${ENV:-default} expansion.
//
// Every setting has a production-safe default, so a minimal config is a listen
// address and a list of endpoints.  Each struct lists its fields once, in
// `visit`, and that single list drives parsing (unknown keys and wrong types
// are errors, not silently ignored), validation messages and the /config dump.
#pragma once

#include <array>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "json.hpp"

namespace jdblance {

class ConfigError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

inline constexpr std::array<std::string_view, 7> kPolicyNames = {
    "round_robin", "weighted_round_robin", "random", "least_conn",
    "p2c_ewma",    "consistent_hash",      "maglev",
};

struct ListenConfig {
  std::string host = "0.0.0.0";
  int port = 8000;
  int backlog = 2048;
  int workers = 1;
  bool reuse_port = true;
  template <class V>
  void visit(V& v) {
    v("host", host); v("port", port); v("backlog", backlog); v("workers", workers);
    v("reuse_port", reuse_port);
  }
  bool operator==(const ListenConfig&) const = default;
};

struct AdminConfig {
  std::string host = "127.0.0.1";
  int port = 8001;
  bool enabled = true;
  template <class V>
  void visit(V& v) { v("host", host); v("port", port); v("enabled", enabled); }
  bool operator==(const AdminConfig&) const = default;
};

struct EndpointConfig {
  std::string name;
  std::string address;   // host:port, IPv6 as [addr]:port
  int weight = 1;
  bool enabled = true;
  template <class V>
  void visit(V& v) { v("name", name); v("address", address); v("weight", weight); v("enabled", enabled); }
  std::string host() const;
  int port() const;
};

struct HealthConfig {
  bool enabled = true;
  std::string path = "/health";
  double interval = 2.0;
  double timeout = 1.0;
  int rise = 2;
  int fall = 3;
  int expected_status = 200;
  std::optional<std::string> expected_body;
  template <class V>
  void visit(V& v) {
    v("enabled", enabled); v("path", path); v("interval", interval); v("timeout", timeout);
    v("rise", rise); v("fall", fall); v("expected_status", expected_status);
    v("expected_body", expected_body);
  }
  bool operator==(const HealthConfig&) const = default;
};

struct BreakerConfig {
  double error_threshold = 0.5;
  int min_requests = 20;
  double window = 10.0;
  double cooldown = 5.0;
  double max_cooldown = 60.0;
  int consecutive_failures = 5;
  int half_open_probes = 1;
  template <class V>
  void visit(V& v) {
    v("error_threshold", error_threshold); v("min_requests", min_requests); v("window", window);
    v("cooldown", cooldown); v("max_cooldown", max_cooldown);
    v("consecutive_failures", consecutive_failures); v("half_open_probes", half_open_probes);
  }
};

struct RetryConfig {
  bool enabled = true;
  int max_attempts = 3;
  double budget_ratio = 0.1;
  double budget_min_per_sec = 3.0;
  int buffer_bytes = 65536;
  bool retry_on_5xx = true;
  double backoff_base = 0.005;
  double backoff_max = 0.1;
  template <class V>
  void visit(V& v) {
    v("enabled", enabled); v("max_attempts", max_attempts); v("budget_ratio", budget_ratio);
    v("budget_min_per_sec", budget_min_per_sec); v("buffer_bytes", buffer_bytes);
    v("retry_on_5xx", retry_on_5xx); v("backoff_base", backoff_base); v("backoff_max", backoff_max);
  }
};

struct HedgeConfig {
  bool enabled = false;
  double quantile = 0.99;
  double min_delay = 0.01;
  double max_delay = 1.0;
  template <class V>
  void visit(V& v) {
    v("enabled", enabled); v("quantile", quantile); v("min_delay", min_delay); v("max_delay", max_delay);
  }
};

// Adaptive concurrency.  `static_limit > 0` pins the limit and disables adaptation.
struct ConcurrencyConfig {
  bool enabled = true;
  int static_limit = 0;
  int initial = 32;
  int min_limit = 4;
  int max_limit = 4096;
  double smoothing = 0.2;
  int max_queue = 1024;
  double codel_target = 0.005;
  double codel_interval = 0.1;
  template <class V>
  void visit(V& v) {
    v("enabled", enabled); v("static_limit", static_limit); v("initial", initial);
    v("min_limit", min_limit); v("max_limit", max_limit); v("smoothing", smoothing);
    v("max_queue", max_queue); v("codel_target", codel_target); v("codel_interval", codel_interval);
  }
};

struct LimitsConfig {
  int max_connections = 8192;
  double rate_per_ip = 0.0;   // 0 disables
  double rate_burst = 0.0;    // 0 means max(rate, 1)
  double max_body_bytes = 64.0 * 1024 * 1024;
  ConcurrencyConfig concurrency;
  template <class V>
  void visit(V& v) {
    v("max_connections", max_connections); v("rate_per_ip", rate_per_ip);
    v("rate_burst", rate_burst); v("max_body_bytes", max_body_bytes); v("concurrency", concurrency);
  }
};

struct TimeoutConfig {
  double connect = 1.0;
  double upstream_header = 10.0;
  double upstream_body = 300.0;
  double client_idle = 65.0;
  double client_header = 10.0;
  double drain = 15.0;
  double tunnel_idle = 300.0;
  template <class V>
  void visit(V& v) {
    v("connect", connect); v("upstream_header", upstream_header); v("upstream_body", upstream_body);
    v("client_idle", client_idle); v("client_header", client_header); v("drain", drain);
    v("tunnel_idle", tunnel_idle);
  }
};

struct PoolConfig {
  bool enabled = true;
  int max_idle_per_endpoint = 64;
  double idle_timeout = 30.0;
  template <class V>
  void visit(V& v) {
    v("enabled", enabled); v("max_idle_per_endpoint", max_idle_per_endpoint);
    v("idle_timeout", idle_timeout);
  }
};

// Where the affinity key comes from: client_ip | header | cookie | path | query.
struct HashKeyConfig {
  std::string source = "client_ip";
  std::string name;
  template <class V>
  void visit(V& v) { v("source", source); v("name", name); }
};

struct LogConfig {
  bool access = true;
  double sample = 1.0;
  std::string level = "info";
  double slow_request = 1.0;
  template <class V>
  void visit(V& v) { v("access", access); v("sample", sample); v("level", level); v("slow_request", slow_request); }
};

struct Config {
  ListenConfig listen;
  AdminConfig admin;
  std::string policy = "p2c_ewma";
  HashKeyConfig hash_key;
  std::vector<EndpointConfig> endpoints;
  HealthConfig health;
  BreakerConfig breaker;
  RetryConfig retry;
  HedgeConfig hedge;
  LimitsConfig limits;
  TimeoutConfig timeouts;
  PoolConfig pool;
  LogConfig log;

  double panic_threshold = 0.5;
  double ewma_tau = 10.0;
  int vnodes = 160;
  double bounded_load_factor = 1.25;
  int maglev_table_size = 65537;
  bool debug_headers = true;
  bool trust_forwarded_for = false;
  std::string server_name = "jdblance/1.0";

  std::string source_path;   // not part of the file format

  template <class V>
  void visit(V& v) {
    v("listen", listen); v("admin", admin); v("policy", policy); v("hash_key", hash_key);
    v("endpoints", endpoints); v("health", health); v("breaker", breaker); v("retry", retry);
    v("hedge", hedge); v("limits", limits); v("timeouts", timeouts); v("pool", pool); v("log", log);
    v("panic_threshold", panic_threshold); v("ewma_tau", ewma_tau); v("vnodes", vnodes);
    v("bounded_load_factor", bounded_load_factor); v("maglev_table_size", maglev_table_size);
    v("debug_headers", debug_headers); v("trust_forwarded_for", trust_forwarded_for);
    v("server_name", server_name);
  }

  // `validate = false` lets the caller apply overrides before validating.
  static Config load(const std::string& path, bool validate = true);
  static Config from_text(std::string_view text, bool validate = true);
  static Config from_json(const Json& data, bool validate = true);
  void validate() const;
  Json to_json() const;
};

// Exposed for tests.
std::string strip_comments(std::string_view raw);
std::string expand_env(std::string_view raw);
bool valid_endpoint_name(std::string_view name) noexcept;

}  // namespace jdblance
