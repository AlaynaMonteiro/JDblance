// HTTP/1.1 wire codec (RFC 9110 / 9112) for the proxy datapath.
//
// Streaming: heads are parsed into memory, bodies never are.  A 4 GiB upload
// costs one 64 KiB relay buffer, and backpressure is end to end because every
// write completes before the next read.
//
// A reverse proxy is a request-smuggling surface: any disagreement between how
// it and the origin frame a message is an exploitable desync.  So this codec
// refuses rather than resolves ambiguity:
//
//   * Content-Length together with Transfer-Encoding -> 400 (CL.TE / TE.CL).
//   * Duplicate Content-Length values that disagree -> 400.
//   * A request Transfer-Encoding other than exactly `chunked` -> 400.  Proxies
//     that "support" `gzip, chunked` by forwarding it re-framed silently strip
//     a coding the origin needed.
//   * obs-fold, whitespace before the colon, non-token header names, control
//     characters (CR, NUL, ...) in values or the request target -> 400.
//   * Chunk sizes: hex digits only, at most 16 of them, hard size cap; the
//     chunk is re-framed on the way out, so extensions never reach the peer.
//   * Hop-by-hop headers and every header named in `Connection:` are stripped
//     before forwarding.
#pragma once

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "loop.hpp"

namespace jdblance::http {

inline constexpr size_t kMaxHeadBytes = 64 * 1024;
inline constexpr size_t kMaxHeaders = 128;
inline constexpr uint64_t kMaxChunkSize = 1ULL << 34;  // 16 GiB; larger is a lie
inline constexpr size_t kRelayChunk = 64 * 1024;
inline constexpr size_t kMaxTarget = 8192;

class ProtocolError : public std::runtime_error {
 public:
  explicit ProtocolError(const std::string& message, int status = 400)
      : std::runtime_error(message), status(status), message(message) {}
  int status;
  std::string message;
};

// A write to the destination of a relay failed.  Kept distinct from read-side
// errors so the caller can tell "the client sent garbage" from "the upstream
// went away" -- they are blamed on different parties.
class SinkError : public std::runtime_error {
 public:
  SinkError(const std::string& what, bool timeout) : std::runtime_error(what), timeout(timeout) {}
  bool timeout;
};

using Header = std::pair<std::string, std::string>;
using Headers = std::vector<Header>;

bool iequals(std::string_view a, std::string_view b) noexcept;
std::string to_lower(std::string_view s);
std::string_view trim(std::string_view s) noexcept;   // SP / HTAB
bool is_token(std::string_view s) noexcept;
const char* status_text(int status) noexcept;

struct Message {
  Headers headers;
  std::string version;
  size_t raw_size = 0;

  const std::string* get(std::string_view name) const noexcept;
  std::string get_or(std::string_view name, std::string fallback = "") const;
  std::vector<std::string> get_all(std::string_view name) const;
  bool has(std::string_view name) const noexcept { return get(name) != nullptr; }
  std::vector<std::string> connection_tokens() const;   // lower-cased
  bool connection_has(std::string_view token) const;
};

struct Request : Message {
  std::string method;
  std::string target;

  std::string_view path() const noexcept;
  std::string_view query() const noexcept;   // without '?'
  bool idempotent() const noexcept;
  bool expects_continue() const;
  std::optional<std::string> upgrade_target() const;
};

struct Response : Message {
  int status = 0;
  std::string reason;
};

enum class BodyKind { None, Length, Chunked, Eof };

struct Framing {
  BodyKind kind = BodyKind::None;
  uint64_t length = 0;
  // Response only: the Transfer-Encoding value to re-emit, so codings applied
  // before `chunked` (or a non-chunked final coding) survive the hop.
  std::string codings;
  bool empty() const noexcept {
    return kind == BodyKind::None || (kind == BodyKind::Length && length == 0);
  }
};

Framing request_framing(const Request& req);
Framing response_framing(std::string_view request_method, const Response& resp);

// Throws ConnectionClosed if the peer closes before sending a byte,
// ProtocolError for malformed heads, TimeoutError / IoError from the socket.
Task<Request> read_request(BufReader& reader, double deadline);
Task<Response> read_response(BufReader& reader, double deadline);

// Value of `name` in a query string, nullopt if absent.
std::optional<std::string> query_param(std::string_view query, std::string_view name);

// Headers minus hop-by-hop, minus everything `Connection:` names, minus `drop`
// (lower-case names).
Headers filtered_headers(const Message& msg, const std::vector<std::string_view>& drop = {});
std::string encode_request(std::string_view method, std::string_view target, const Headers& headers);
std::string encode_response(int status, const Headers& headers, std::string_view reason = "");
std::string simple_response(int status, std::string_view body,
                            std::string_view content_type = "text/plain; charset=utf-8",
                            bool close = true, const Headers& extra = {});

// Relay destination: a socket, an in-memory buffer, or (both null) discard.
struct Sink {
  Socket* socket = nullptr;
  std::string* collect = nullptr;
};

// Copy one message body.  Returns payload bytes moved.  ProtocolError(413) if
// `limit` is exceeded; SinkError if writing to the sink fails.
// `dechunk` writes only the payload of a chunked body (for HTTP/1.0 peers,
// which cannot parse chunked framing).
Task<uint64_t> relay_body(BufReader& reader, Sink sink, Framing framing,
                          std::optional<uint64_t> limit, double deadline, bool dechunk = false);

// Buffer and *decode* a body so the request stays retryable; chunked input is
// returned de-chunked.  nullopt when the body is known up front to exceed
// `max_bytes` (nothing consumed); a chunked body that turns out too big is 413.
Task<std::optional<std::string>> read_body_into_memory(BufReader& reader, Framing framing,
                                                       size_t max_bytes, double deadline);

// A tiny client for tools and tests: one request on a fresh connection.
struct FetchResult {
  Response response;
  std::string body;
};
Task<FetchResult> fetch(std::string host, int port, std::string method, std::string target,
                        Headers headers, std::string body, double timeout);
// Sends `raw` verbatim and reads one response (framed as if for `method`).
Task<FetchResult> fetch_raw(std::string host, int port, std::string raw, std::string method,
                            double timeout);

}  // namespace jdblance::http
