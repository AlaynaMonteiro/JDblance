// Runtime: C++20 coroutines over a single-threaded epoll reactor.
//
// One Loop per process (per worker).  Everything that touches proxy state runs
// on the loop thread, so the datapath needs no locks.  The only other threads are
// short-lived DNS resolvers, which talk back through a thread-safe inbox.
//
// Rules that keep this safe:
//   * A coroutine that is spawned (detached) takes its parameters by value or
//     shared_ptr.  A coroutine that is co_awaited immediately may take
//     references: the caller's frame outlives it.
//   * Every suspension has exactly one wake-up path.  A `Wait` is detached from
//     its fd/event slot and its timer before the coroutine is posted, so a
//     timeout and a readiness event can never both resume it.
//   * Never co_await inside a catch block (the language forbids it); record the
//     error and act after the try.
#pragma once

#include <netinet/in.h>
#include <sys/socket.h>

#include <coroutine>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace jdblance {

double monotonic() noexcept;   // seconds, CLOCK_MONOTONIC
double wall_time() noexcept;   // seconds since the epoch
inline constexpr double kForever = std::numeric_limits<double>::infinity();

// Raise the soft open-file limit to the hard limit.  A proxy holds two
// sockets per request; the common soft default of 1024 is a few hundred
// concurrent requests.  Returns the resulting soft limit.
long raise_fd_limit() noexcept;
// Errors that mean *this process* ran out of something, not that a peer failed.
bool is_local_resource_error(int code) noexcept;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

class IoError : public std::runtime_error {
 public:
  IoError(int code, const std::string& context);
  int code() const noexcept { return code_; }

 private:
  int code_;
};

class TimeoutError : public std::runtime_error {
 public:
  TimeoutError() : std::runtime_error("timeout") {}
};

class CancelledError : public std::runtime_error {
 public:
  CancelledError() : std::runtime_error("cancelled") {}
};

// The peer closed before sending anything (a clean keep-alive close).
class ConnectionClosed : public std::runtime_error {
 public:
  ConnectionClosed() : std::runtime_error("connection closed") {}
};

// ---------------------------------------------------------------------------
// Task<T>: a lazy coroutine.  `co_await task` runs it; `spawn(task)` detaches.
// ---------------------------------------------------------------------------

template <typename T = void>
class Task;

namespace detail {

void report_detached_exception(std::exception_ptr error) noexcept;

struct PromiseBase {
  std::coroutine_handle<> continuation;
  std::exception_ptr error;
  bool detached = false;

  std::suspend_always initial_suspend() noexcept { return {}; }

  struct FinalAwaiter {
    bool await_ready() noexcept { return false; }
    template <typename P>
    std::coroutine_handle<> await_suspend(std::coroutine_handle<P> h) noexcept {
      PromiseBase& p = h.promise();
      if (p.detached) {
        std::exception_ptr error = p.error;
        h.destroy();
        if (error) report_detached_exception(error);
        return std::noop_coroutine();
      }
      return p.continuation ? p.continuation : std::noop_coroutine();
    }
    void await_resume() noexcept {}
  };

  FinalAwaiter final_suspend() noexcept { return {}; }
  void unhandled_exception() noexcept { error = std::current_exception(); }
};

}  // namespace detail

template <typename T>
class [[nodiscard]] Task {
 public:
  struct promise_type : detail::PromiseBase {
    std::optional<T> value;
    Task get_return_object() noexcept {
      return Task(std::coroutine_handle<promise_type>::from_promise(*this));
    }
    template <typename U>
    void return_value(U&& v) {
      value.emplace(std::forward<U>(v));
    }
  };
  using Handle = std::coroutine_handle<promise_type>;

  Task() = default;
  explicit Task(Handle h) noexcept : h_(h) {}
  Task(Task&& other) noexcept : h_(std::exchange(other.h_, {})) {}
  Task& operator=(Task&& other) noexcept {
    if (this != &other) {
      reset();
      h_ = std::exchange(other.h_, {});
    }
    return *this;
  }
  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;
  ~Task() { reset(); }

  bool await_ready() const noexcept { return false; }
  std::coroutine_handle<> await_suspend(std::coroutine_handle<> cont) noexcept {
    h_.promise().continuation = cont;
    return h_;
  }
  T await_resume() {
    auto& p = h_.promise();
    if (p.error) std::rethrow_exception(p.error);
    return std::move(*p.value);
  }
  Handle release() noexcept { return std::exchange(h_, {}); }

 private:
  void reset() noexcept {
    if (h_) {
      h_.destroy();
      h_ = {};
    }
  }
  Handle h_;
};

template <>
class [[nodiscard]] Task<void> {
 public:
  struct promise_type : detail::PromiseBase {
    Task get_return_object() noexcept {
      return Task(std::coroutine_handle<promise_type>::from_promise(*this));
    }
    void return_void() noexcept {}
  };
  using Handle = std::coroutine_handle<promise_type>;

  Task() = default;
  explicit Task(Handle h) noexcept : h_(h) {}
  Task(Task&& other) noexcept : h_(std::exchange(other.h_, {})) {}
  Task& operator=(Task&& other) noexcept {
    if (this != &other) {
      reset();
      h_ = std::exchange(other.h_, {});
    }
    return *this;
  }
  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;
  ~Task() { reset(); }

  bool await_ready() const noexcept { return false; }
  std::coroutine_handle<> await_suspend(std::coroutine_handle<> cont) noexcept {
    h_.promise().continuation = cont;
    return h_;
  }
  void await_resume() {
    if (h_.promise().error) std::rethrow_exception(h_.promise().error);
  }
  Handle release() noexcept { return std::exchange(h_, {}); }

 private:
  void reset() noexcept {
    if (h_) {
      h_.destroy();
      h_ = {};
    }
  }
  Handle h_;
};

// ---------------------------------------------------------------------------
// Loop
// ---------------------------------------------------------------------------

class Loop;
struct Wait;

// Per-fd registration.  `reader`/`writer` point at the Wait currently parked
// on that direction; `on_readable` is for loop-internal fds (inbox, signals).
struct FdState {
  Wait* reader = nullptr;
  Wait* writer = nullptr;
  std::function<void()> on_readable;
};

// Cross-thread mailbox: the only thing other threads may touch.
struct Inbox {
  Inbox();
  ~Inbox();
  void push(std::function<void()> fn);
  std::vector<std::function<void()>> take();
  int fd;

 private:
  std::mutex mu_;
  std::vector<std::function<void()>> items_;
};

class Loop {
 public:
  using TimerMap = std::multimap<double, std::function<void()>>;
  using TimerId = TimerMap::iterator;

  Loop();
  ~Loop();
  Loop(const Loop&) = delete;
  Loop& operator=(const Loop&) = delete;

  static Loop& current();

  void post(std::coroutine_handle<> h) { ready_.push_back(h); }
  TimerId add_timer(double when, std::function<void()> fn);
  void cancel_timer(TimerId id) { timers_.erase(id); }

  void run();   // until stop()
  void stop() { stop_ = true; }

  void add_fd(int fd, FdState* state);
  void del_fd(int fd) noexcept;

  std::shared_ptr<Inbox> inbox() const { return inbox_; }

  // Drive `task` to completion on this loop and return its result.  For
  // tests, tools and startup code -- never call it from inside a coroutine.
  template <typename T>
  T run_until_complete(Task<T> task);

 private:
  void dispatch_timers();
  int epfd_ = -1;
  bool stop_ = false;
  std::deque<std::coroutine_handle<>> ready_;
  TimerMap timers_;
  std::shared_ptr<Inbox> inbox_;
  FdState inbox_state_;
  Loop* previous_ = nullptr;
};

// One parked coroutine.  Lives in the awaiting coroutine's frame.
struct Wait {
  enum Result { kPending, kReady, kTimeout, kCancelled };

  std::coroutine_handle<> handle;
  Result result = kPending;
  Wait** slot = nullptr;
  Loop* loop = nullptr;
  bool has_timer = false;
  Loop::TimerId timer;

  void arm(Loop& l, std::coroutine_handle<> h, Wait** where, double deadline);
  void fire(Result r) noexcept;
};

struct WaitAwaiter {
  Loop& loop;
  Wait** slot;
  double deadline;
  bool immediate;      // skip suspension (already cancelled)
  Wait wait;

  bool await_ready() const noexcept { return immediate; }
  void await_suspend(std::coroutine_handle<> h) { wait.arm(loop, h, slot, deadline); }
  Wait::Result await_resume() const noexcept { return immediate ? Wait::kCancelled : wait.result; }
};

// Detach `task` onto the current loop.  It starts on the next loop turn.
void spawn(Task<void> task);

// Uncancellable sleep.  Only for short waits (retry backoff, polling).
struct SleepAwaiter {
  double when;
  bool await_ready() const noexcept { return when <= monotonic(); }
  void await_suspend(std::coroutine_handle<> h);
  void await_resume() const noexcept {}
};
inline SleepAwaiter sleep_for(double seconds) { return SleepAwaiter{monotonic() + seconds}; }

// One-shot-or-reusable flag with at most one waiter.  `co_await ev.wait(d)`
// returns true if the event is (or becomes) set, false on timeout.
class Event {
 public:
  Event() = default;
  Event(const Event&) = delete;
  Event& operator=(const Event&) = delete;
  ~Event();

  void set() noexcept;
  void clear() noexcept { set_ = false; }
  bool is_set() const noexcept { return set_; }

  struct Awaiter {
    Event& ev;
    double deadline;
    Wait wait;
    bool await_ready() const noexcept { return ev.set_; }
    void await_suspend(std::coroutine_handle<> h) {
      wait.arm(Loop::current(), h, &ev.waiter_, deadline);
    }
    bool await_resume() const noexcept {
      return wait.result == Wait::kPending || wait.result == Wait::kReady;
    }
  };
  Awaiter wait(double deadline = kForever) { return Awaiter{*this, deadline, {}}; }

 private:
  bool set_ = false;
  Wait* waiter_ = nullptr;
};

// ---------------------------------------------------------------------------
// Addresses and sockets
// ---------------------------------------------------------------------------

struct Address {
  sockaddr_storage storage{};
  socklen_t length = 0;
  std::string to_string() const;
};

// Literal IPv4/IPv6 (brackets allowed) -> address, without touching DNS.
std::optional<Address> parse_numeric_address(const std::string& host, int port);
// Async resolution: getaddrinfo runs on a helper thread.
Task<std::vector<Address>> resolve(std::string host, int port, double deadline);
// Blocking resolution, for startup only.
std::vector<Address> resolve_blocking(const std::string& host, int port, bool passive);

class Socket {
 public:
  explicit Socket(int fd);   // takes ownership; makes it non-blocking
  ~Socket();
  Socket(const Socket&) = delete;
  Socket& operator=(const Socket&) = delete;

  int fd() const noexcept { return fd_; }
  bool closed() const noexcept { return fd_ < 0; }
  bool cancelled() const noexcept { return cancelled_; }

  // 0 means EOF.  Throws IoError / TimeoutError / CancelledError.
  Task<size_t> read_some(char* buf, size_t n, double deadline);
  Task<void> write_all(std::string_view data, double deadline);
  Task<void> wait_readable(double deadline);
  Task<void> wait_writable(double deadline);

  // Wake any parked operation with CancelledError and make future ones fail.
  void cancel() noexcept;
  void close() noexcept;
  void shutdown_write() noexcept;
  // For pooled idle sockets: true if the peer has not closed or sent bytes.
  bool idle_and_open() const noexcept;
  void set_nodelay() noexcept;
  std::string peer_ip() const;
  int local_port() const;

 private:
  WaitAwaiter park(bool write, double deadline);
  int fd_;
  bool cancelled_ = false;
  Loop* loop_;
  FdState state_;
};

using SocketPtr = std::shared_ptr<Socket>;

Task<SocketPtr> connect_tcp(Address address, double deadline);
// Tries each address in order (e.g. ::1 then 127.0.0.1 for "localhost").
Task<SocketPtr> connect_any(std::vector<Address> addresses, double deadline);
// A bound, listening, non-blocking socket.  Throws IoError.
int listen_tcp(const std::string& host, int port, int backlog, bool reuse_port);
// Waits for the next connection; throws CancelledError once the listener is cancelled.
Task<SocketPtr> accept_one(Socket& listener);

// ---------------------------------------------------------------------------
// Buffered reader
// ---------------------------------------------------------------------------

class LineTooLong : public std::runtime_error {
 public:
  LineTooLong() : std::runtime_error("line too long") {}
};

class IncompleteRead : public std::runtime_error {
 public:
  IncompleteRead() : std::runtime_error("incomplete read") {}
};

// Allocator whose value-initialisation is default-initialisation, so sizing a
// byte buffer does not zero memory that read() is about to overwrite.  A
// BufReader is created per upstream attempt; zeroing its 8 KiB every time
// was measurable.
template <typename T>
struct UninitAllocator : std::allocator<T> {
  template <typename U>
  struct rebind {
    using other = UninitAllocator<U>;
  };
  UninitAllocator() = default;
  template <typename U>
  UninitAllocator(const UninitAllocator<U>&) noexcept {}
  template <typename U>
  void construct(U* p) noexcept(std::is_nothrow_default_constructible_v<U>) {
    ::new (static_cast<void*>(p)) U;
  }
  template <typename U, typename... Args>
  void construct(U* p, Args&&... args) {
    ::new (static_cast<void*>(p)) U(std::forward<Args>(args)...);
  }
};

class BufReader {
 public:
  explicit BufReader(SocketPtr socket);
  explicit BufReader(std::string data);   // in-memory source; EOF after data

  // Line including its '\n'; "" at EOF; a partial line at EOF is returned as-is.
  Task<std::string> read_line(size_t max_length, double deadline);
  // Exactly n bytes or IncompleteRead.
  Task<std::string> read_exact(size_t n, double deadline);
  // Copies up to `max` bytes into `dst`; returns the count, 0 at EOF.
  Task<size_t> read_into(char* dst, size_t max, double deadline);

  size_t buffered() const noexcept { return end_ - start_; }
  const SocketPtr& socket() const noexcept { return socket_; }

 private:
  Task<bool> fill(double deadline);
  SocketPtr socket_;
  std::vector<char, UninitAllocator<char>> buf_;
  size_t start_ = 0;
  size_t end_ = 0;
  bool eof_ = false;
};

// ---------------------------------------------------------------------------
// run_until_complete
// ---------------------------------------------------------------------------

namespace detail {
template <typename T>
Task<void> run_wrapper(Task<T> task, std::optional<T>* out, std::exception_ptr* error, Loop* loop) {
  try {
    out->emplace(co_await task);
  } catch (...) {
    *error = std::current_exception();
  }
  loop->stop();
}
inline Task<void> run_wrapper_void(Task<void> task, bool* done, std::exception_ptr* error,
                                   Loop* loop) {
  try {
    co_await task;
  } catch (...) {
    *error = std::current_exception();
  }
  *done = true;
  loop->stop();
}
}  // namespace detail

template <typename T>
T Loop::run_until_complete(Task<T> task) {
  if constexpr (std::is_void_v<T>) {
    bool done = false;
    std::exception_ptr error;
    auto h = detail::run_wrapper_void(std::move(task), &done, &error, this).release();
    h.promise().detached = true;
    post(h);
    while (!done) run();
    if (error) std::rethrow_exception(error);
  } else {
    std::optional<T> out;
    std::exception_ptr error;
    auto h = detail::run_wrapper<T>(std::move(task), &out, &error, this).release();
    h.promise().detached = true;
    post(h);
    while (!out && !error) run();
    if (error) std::rethrow_exception(error);
    return std::move(*out);
  }
}

}  // namespace jdblance
