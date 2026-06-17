#include "loop.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

#include <cerrno>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <thread>

namespace jdblance {

double monotonic() noexcept {
  timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<double>(ts.tv_sec) + static_cast<double>(ts.tv_nsec) * 1e-9;
}

double wall_time() noexcept {
  timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return static_cast<double>(ts.tv_sec) + static_cast<double>(ts.tv_nsec) * 1e-9;
}

long raise_fd_limit() noexcept {
  rlimit lim{};
  if (::getrlimit(RLIMIT_NOFILE, &lim) != 0) return -1;
  if (lim.rlim_cur < lim.rlim_max) {
    rlimit want = lim;
    want.rlim_cur = lim.rlim_max;
    if (::setrlimit(RLIMIT_NOFILE, &want) == 0) lim = want;
  }
  return lim.rlim_cur == RLIM_INFINITY ? -1 : static_cast<long>(lim.rlim_cur);
}

bool is_local_resource_error(int code) noexcept {
  return code == EMFILE || code == ENFILE || code == ENOBUFS || code == ENOMEM ||
         code == EADDRNOTAVAIL;
}

IoError::IoError(int code, const std::string& context)
    : std::runtime_error(context + ": " + std::strerror(code)), code_(code) {}

namespace detail {
void report_detached_exception(std::exception_ptr error) noexcept {
  try {
    std::rethrow_exception(error);
  } catch (const std::exception& e) {
    std::fprintf(stderr, "jdblance: unhandled exception in detached task: %s\n", e.what());
  } catch (...) {
    std::fprintf(stderr, "jdblance: unhandled non-standard exception in detached task\n");
  }
}
}  // namespace detail

// ---------------------------------------------------------------------------
// Inbox
// ---------------------------------------------------------------------------

Inbox::Inbox() : fd(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) {
  if (fd < 0) throw IoError(errno, "eventfd");
}

Inbox::~Inbox() { ::close(fd); }

void Inbox::push(std::function<void()> fn) {
  {
    std::lock_guard<std::mutex> lock(mu_);
    items_.push_back(std::move(fn));
  }
  uint64_t one = 1;
  // EAGAIN only if the counter would overflow, in which case it is already
  // non-zero and the loop will wake anyway.
  [[maybe_unused]] ssize_t ignored = ::write(fd, &one, sizeof one);
}

std::vector<std::function<void()>> Inbox::take() {
  uint64_t counter;
  [[maybe_unused]] ssize_t ignored = ::read(fd, &counter, sizeof counter);
  std::lock_guard<std::mutex> lock(mu_);
  return std::exchange(items_, {});
}

// ---------------------------------------------------------------------------
// Loop
// ---------------------------------------------------------------------------

namespace {
thread_local Loop* g_current = nullptr;

int wait_events(int epfd, epoll_event* events, int max, double timeout) {
  static bool have_pwait2 = true;
  if (timeout > 3600) timeout = 3600;
  if (have_pwait2) {
    timespec ts{};
    timespec* tsp = nullptr;
    if (timeout >= 0) {
      ts.tv_sec = static_cast<time_t>(timeout);
      ts.tv_nsec = static_cast<long>((timeout - static_cast<double>(ts.tv_sec)) * 1e9);
      tsp = &ts;
    }
    int n = ::epoll_pwait2(epfd, events, max, tsp, nullptr);
    if (n >= 0 || errno != ENOSYS) return n;
    have_pwait2 = false;  // pre-5.11 kernel: millisecond timers it is
  }
  int ms = timeout < 0 ? -1 : static_cast<int>(std::ceil(timeout * 1000.0));
  return ::epoll_wait(epfd, events, max, ms);
}
}  // namespace

Loop::Loop() {
  // A peer resetting a socket we write to must surface as EPIPE, not kill us.
  ::signal(SIGPIPE, SIG_IGN);
  epfd_ = ::epoll_create1(EPOLL_CLOEXEC);
  if (epfd_ < 0) throw IoError(errno, "epoll_create1");
  inbox_ = std::make_shared<Inbox>();
  inbox_state_.on_readable = [this] {
    for (auto& fn : inbox_->take()) fn();
  };
  add_fd(inbox_->fd, &inbox_state_);
  previous_ = g_current;
  g_current = this;
}

Loop::~Loop() {
  del_fd(inbox_->fd);
  ::close(epfd_);
  g_current = previous_;
}

Loop& Loop::current() {
  if (g_current == nullptr) throw std::logic_error("no event loop on this thread");
  return *g_current;
}

Loop::TimerId Loop::add_timer(double when, std::function<void()> fn) {
  return timers_.emplace(when, std::move(fn));
}

void Loop::add_fd(int fd, FdState* state) {
  epoll_event ev{};
  ev.events = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET;
  ev.data.ptr = state;
  if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) < 0) throw IoError(errno, "epoll_ctl");
}

void Loop::del_fd(int fd) noexcept { ::epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr); }

void Loop::dispatch_timers() {
  if (timers_.empty()) return;
  double now = monotonic();
  while (!timers_.empty() && timers_.begin()->first <= now) {
    auto node = timers_.extract(timers_.begin());
    node.mapped()();
  }
}

void Loop::run() {
  stop_ = false;
  epoll_event events[256];
  while (!stop_) {
    // Only what is ready now; anything posted while running waits a turn, so
    // a coroutine that keeps re-posting cannot starve I/O.
    for (size_t n = ready_.size(); n > 0 && !stop_ && !ready_.empty(); --n) {
      auto h = ready_.front();
      ready_.pop_front();
      h.resume();
    }
    if (stop_) break;
    dispatch_timers();

    double timeout = -1;
    if (!ready_.empty()) {
      timeout = 0;
    } else if (!timers_.empty()) {
      timeout = std::max(0.0, timers_.begin()->first - monotonic());
    }
    int count = wait_events(epfd_, events, 256, timeout);
    if (count < 0 && errno != EINTR) throw IoError(errno, "epoll_wait");
    // Dispatch only posts coroutines (never runs them), so no FdState can be
    // freed while this batch is being walked.
    for (int i = 0; i < count; ++i) {
      auto* state = static_cast<FdState*>(events[i].data.ptr);
      uint32_t ev = events[i].events;
      if (state->on_readable) {
        if (ev & (EPOLLIN | EPOLLERR | EPOLLHUP)) state->on_readable();
        continue;
      }
      if ((ev & (EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP)) && state->reader) {
        state->reader->fire(Wait::kReady);
      }
      if ((ev & (EPOLLOUT | EPOLLERR | EPOLLHUP)) && state->writer) {
        state->writer->fire(Wait::kReady);
      }
    }
    dispatch_timers();
  }
}

void Wait::arm(Loop& l, std::coroutine_handle<> h, Wait** where, double deadline) {
  loop = &l;
  handle = h;
  slot = where;
  result = kPending;
  if (where != nullptr) *where = this;
  if (deadline != kForever) {
    timer = l.add_timer(deadline, [this] {
      has_timer = false;
      fire(kTimeout);
    });
    has_timer = true;
  }
}

void Wait::fire(Result r) noexcept {
  if (result != kPending) return;
  result = r;
  if (has_timer) {
    loop->cancel_timer(timer);
    has_timer = false;
  }
  if (slot != nullptr && *slot == this) *slot = nullptr;
  slot = nullptr;
  loop->post(handle);
}

void spawn(Task<void> task) {
  auto h = task.release();
  h.promise().detached = true;
  Loop::current().post(h);
}

void SleepAwaiter::await_suspend(std::coroutine_handle<> h) {
  Loop& loop = Loop::current();
  loop.add_timer(when, [h, &loop] { loop.post(h); });
}

Event::~Event() {
  if (waiter_ != nullptr) waiter_->fire(Wait::kCancelled);
}

void Event::set() noexcept {
  set_ = true;
  if (waiter_ != nullptr) waiter_->fire(Wait::kReady);
}

// ---------------------------------------------------------------------------
// Addresses
// ---------------------------------------------------------------------------

std::string Address::to_string() const {
  char host[INET6_ADDRSTRLEN] = "?";
  int port = 0;
  if (storage.ss_family == AF_INET) {
    auto* sin = reinterpret_cast<const sockaddr_in*>(&storage);
    ::inet_ntop(AF_INET, &sin->sin_addr, host, sizeof host);
    port = ntohs(sin->sin_port);
    return std::string(host) + ":" + std::to_string(port);
  }
  if (storage.ss_family == AF_INET6) {
    auto* sin6 = reinterpret_cast<const sockaddr_in6*>(&storage);
    ::inet_ntop(AF_INET6, &sin6->sin6_addr, host, sizeof host);
    port = ntohs(sin6->sin6_port);
    return "[" + std::string(host) + "]:" + std::to_string(port);
  }
  return "?";
}

std::optional<Address> parse_numeric_address(const std::string& raw_host, int port) {
  std::string host = raw_host;
  if (host.size() >= 2 && host.front() == '[' && host.back() == ']') {
    host = host.substr(1, host.size() - 2);
  }
  Address a;
  auto* sin = reinterpret_cast<sockaddr_in*>(&a.storage);
  if (::inet_pton(AF_INET, host.c_str(), &sin->sin_addr) == 1) {
    sin->sin_family = AF_INET;
    sin->sin_port = htons(static_cast<uint16_t>(port));
    a.length = sizeof(sockaddr_in);
    return a;
  }
  auto* sin6 = reinterpret_cast<sockaddr_in6*>(&a.storage);
  if (::inet_pton(AF_INET6, host.c_str(), &sin6->sin6_addr) == 1) {
    sin6->sin6_family = AF_INET6;
    sin6->sin6_port = htons(static_cast<uint16_t>(port));
    a.length = sizeof(sockaddr_in6);
    return a;
  }
  return std::nullopt;
}

namespace {
std::vector<Address> getaddrinfo_tcp(const std::string& raw_host, int port, bool passive,
                                     int* error) {
  std::string host = raw_host;
  if (host.size() >= 2 && host.front() == '[' && host.back() == ']') {
    host = host.substr(1, host.size() - 2);
  }
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = passive ? AI_PASSIVE : AI_ADDRCONFIG;
  addrinfo* res = nullptr;
  std::string service = std::to_string(port);
  int rc = ::getaddrinfo(host.empty() ? nullptr : host.c_str(), service.c_str(), &hints, &res);
  std::vector<Address> out;
  if (rc != 0) {
    *error = rc;
    return out;
  }
  for (addrinfo* p = res; p != nullptr; p = p->ai_next) {
    if (p->ai_addrlen > sizeof(sockaddr_storage)) continue;
    Address a;
    std::memcpy(&a.storage, p->ai_addr, p->ai_addrlen);
    a.length = static_cast<socklen_t>(p->ai_addrlen);
    out.push_back(a);
  }
  ::freeaddrinfo(res);
  *error = 0;
  return out;
}

struct ResolveState {
  Event done;
  std::vector<Address> result;
  int error = 0;
};
}  // namespace

std::vector<Address> resolve_blocking(const std::string& host, int port, bool passive) {
  if (auto a = parse_numeric_address(host, port)) return {*a};
  int error = 0;
  auto out = getaddrinfo_tcp(host, port, passive, &error);
  if (out.empty()) {
    throw std::runtime_error("cannot resolve " + host + ": " +
                             (error ? ::gai_strerror(error) : "no addresses"));
  }
  return out;
}

Task<std::vector<Address>> resolve(std::string host, int port, double deadline) {
  if (auto a = parse_numeric_address(host, port)) co_return std::vector<Address>{*a};
  auto state = std::make_shared<ResolveState>();
  std::shared_ptr<Inbox> inbox = Loop::current().inbox();
  std::thread([state, inbox, host, port] {
    int error = 0;
    std::vector<Address> out = getaddrinfo_tcp(host, port, false, &error);
    inbox->push([state, out = std::move(out), error]() mutable {
      state->result = std::move(out);
      state->error = error;
      state->done.set();
    });
  }).detach();
  bool done = co_await state->done.wait(deadline);
  if (!done) throw TimeoutError();
  if (state->result.empty()) {
    throw IoError(EHOSTUNREACH, "resolve " + host + " (" +
                                    (state->error ? ::gai_strerror(state->error) : "no addresses") +
                                    ")");
  }
  co_return std::move(state->result);
}

// ---------------------------------------------------------------------------
// Socket
// ---------------------------------------------------------------------------

Socket::Socket(int fd) : fd_(fd), loop_(&Loop::current()) {
  int flags = ::fcntl(fd_, F_GETFL, 0);
  if (flags >= 0 && !(flags & O_NONBLOCK)) ::fcntl(fd_, F_SETFL, flags | O_NONBLOCK);
  try {
    loop_->add_fd(fd_, &state_);
  } catch (...) {
    ::close(fd_);
    throw;
  }
}

Socket::~Socket() { close(); }

void Socket::cancel() noexcept {
  cancelled_ = true;
  if (state_.reader != nullptr) state_.reader->fire(Wait::kCancelled);
  if (state_.writer != nullptr) state_.writer->fire(Wait::kCancelled);
}

void Socket::close() noexcept {
  if (fd_ < 0) return;
  cancel();
  loop_->del_fd(fd_);
  ::close(fd_);
  fd_ = -1;
}

void Socket::shutdown_write() noexcept {
  if (fd_ >= 0) ::shutdown(fd_, SHUT_WR);
}

bool Socket::idle_and_open() const noexcept {
  if (fd_ < 0 || cancelled_) return false;
  char probe;
  ssize_t n = ::recv(fd_, &probe, 1, MSG_PEEK | MSG_DONTWAIT);
  // 0 = peer closed; >0 = unsolicited bytes, which would corrupt the next
  // response read off this socket.  Only "nothing to read" is reusable.
  return n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK);
}

void Socket::set_nodelay() noexcept {
  // A proxy writes a head and then a body; with Nagle on, every small
  // response waits an extra RTT for the ACK of the head.
  int one = 1;
  ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
}

std::string Socket::peer_ip() const {
  sockaddr_storage ss{};
  socklen_t len = sizeof ss;
  if (::getpeername(fd_, reinterpret_cast<sockaddr*>(&ss), &len) < 0) return "-";
  char host[INET6_ADDRSTRLEN] = "-";
  if (ss.ss_family == AF_INET) {
    ::inet_ntop(AF_INET, &reinterpret_cast<sockaddr_in*>(&ss)->sin_addr, host, sizeof host);
  } else if (ss.ss_family == AF_INET6) {
    auto* sin6 = reinterpret_cast<sockaddr_in6*>(&ss);
    if (IN6_IS_ADDR_V4MAPPED(&sin6->sin6_addr)) {
      ::inet_ntop(AF_INET, &sin6->sin6_addr.s6_addr[12], host, sizeof host);
    } else {
      ::inet_ntop(AF_INET6, &sin6->sin6_addr, host, sizeof host);
    }
  }
  return host;
}

int Socket::local_port() const {
  sockaddr_storage ss{};
  socklen_t len = sizeof ss;
  if (::getsockname(fd_, reinterpret_cast<sockaddr*>(&ss), &len) < 0) return -1;
  if (ss.ss_family == AF_INET) return ntohs(reinterpret_cast<sockaddr_in*>(&ss)->sin_port);
  if (ss.ss_family == AF_INET6) return ntohs(reinterpret_cast<sockaddr_in6*>(&ss)->sin6_port);
  return -1;
}

WaitAwaiter Socket::park(bool write, double deadline) {
  Wait** slot = write ? &state_.writer : &state_.reader;
  if (*slot != nullptr) throw std::logic_error("two coroutines waiting on one socket direction");
  return WaitAwaiter{*loop_, slot, deadline, cancelled_ || fd_ < 0, {}};
}

Task<size_t> Socket::read_some(char* buf, size_t n, double deadline) {
  for (;;) {
    if (cancelled_) throw CancelledError();
    ssize_t r = ::read(fd_, buf, n);
    if (r >= 0) co_return static_cast<size_t>(r);
    if (errno == EINTR) continue;
    if (errno != EAGAIN && errno != EWOULDBLOCK) throw IoError(errno, "read");
    Wait::Result res = co_await park(false, deadline);
    if (res == Wait::kTimeout) throw TimeoutError();
    if (res == Wait::kCancelled) throw CancelledError();
  }
}

Task<void> Socket::write_all(std::string_view data, double deadline) {
  size_t off = 0;
  while (off < data.size()) {
    if (cancelled_) throw CancelledError();
    ssize_t r = ::send(fd_, data.data() + off, data.size() - off, MSG_NOSIGNAL);
    if (r >= 0) {
      off += static_cast<size_t>(r);
      continue;
    }
    if (errno == EINTR) continue;
    if (errno != EAGAIN && errno != EWOULDBLOCK) throw IoError(errno, "write");
    Wait::Result res = co_await park(true, deadline);
    if (res == Wait::kTimeout) throw TimeoutError();
    if (res == Wait::kCancelled) throw CancelledError();
  }
}

Task<void> Socket::wait_readable(double deadline) {
  if (cancelled_) throw CancelledError();
  Wait::Result res = co_await park(false, deadline);
  if (res == Wait::kTimeout) throw TimeoutError();
  if (res == Wait::kCancelled) throw CancelledError();
}

Task<void> Socket::wait_writable(double deadline) {
  if (cancelled_) throw CancelledError();
  Wait::Result res = co_await park(true, deadline);
  if (res == Wait::kTimeout) throw TimeoutError();
  if (res == Wait::kCancelled) throw CancelledError();
}

Task<SocketPtr> connect_tcp(Address address, double deadline) {
  int fd = ::socket(address.storage.ss_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd < 0) throw IoError(errno, "socket");
  int rc = ::connect(fd, reinterpret_cast<const sockaddr*>(&address.storage), address.length);
  if (rc < 0 && errno != EINPROGRESS) {
    int err = errno;
    ::close(fd);
    throw IoError(err, "connect " + address.to_string());
  }
  // Registered after connect(), so the first EPOLLOUT edge means "connected or failed".
  auto sock = std::make_shared<Socket>(fd);
  while (rc < 0) {
    co_await sock->wait_writable(deadline);
    int err = 0;
    socklen_t len = sizeof err;
    ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
    if (err != 0) throw IoError(err, "connect " + address.to_string());
    sockaddr_storage peer{};
    socklen_t plen = sizeof peer;
    if (::getpeername(fd, reinterpret_cast<sockaddr*>(&peer), &plen) == 0) break;
    if (errno != ENOTCONN) throw IoError(errno, "connect " + address.to_string());
    // Spurious wake-up while still in SYN_SENT: park again.
  }
  sock->set_nodelay();
  co_return sock;
}

Task<SocketPtr> connect_any(std::vector<Address> addresses, double deadline) {
  std::exception_ptr last;
  for (const Address& a : addresses) {
    try {
      co_return co_await connect_tcp(a, deadline);
    } catch (const IoError&) {
      last = std::current_exception();
    }
  }
  if (last) std::rethrow_exception(last);
  throw IoError(EHOSTUNREACH, "connect: no addresses");
}

int listen_tcp(const std::string& host, int port, int backlog, bool reuse_port) {
  std::vector<Address> addrs = resolve_blocking(host, port, true);
  int last_error = 0;
  for (const Address& a : addrs) {
    int fd = ::socket(a.storage.ss_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
      last_error = errno;
      continue;
    }
    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    if (reuse_port) ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof one);
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    if (::bind(fd, reinterpret_cast<const sockaddr*>(&a.storage), a.length) == 0 &&
        ::listen(fd, backlog) == 0) {
      return fd;
    }
    last_error = errno;
    ::close(fd);
  }
  throw IoError(last_error ? last_error : EADDRNOTAVAIL,
                "listen " + host + ":" + std::to_string(port));
}

Task<SocketPtr> accept_one(Socket& listener) {
  for (;;) {
    if (listener.cancelled() || listener.closed()) throw CancelledError();
    int fd = ::accept4(listener.fd(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (fd >= 0) {
      auto sock = std::make_shared<Socket>(fd);
      sock->set_nodelay();
      co_return sock;
    }
    int err = errno;
    if (err == EINTR || err == ECONNABORTED || err == EPROTO) continue;
    if (err == EAGAIN || err == EWOULDBLOCK) {
      co_await listener.wait_readable(kForever);
      continue;
    }
    if (err == EMFILE || err == ENFILE || err == ENOBUFS || err == ENOMEM) {
      // Out of descriptors: the connection stays queued in the kernel.
      // Back off rather than spin on an accept that cannot succeed.
      std::fprintf(stderr, "jdblance: accept: %s; backing off\n", std::strerror(err));
      co_await sleep_for(0.05);
      continue;
    }
    throw IoError(err, "accept");
  }
}

// ---------------------------------------------------------------------------
// BufReader
// ---------------------------------------------------------------------------

namespace {
constexpr size_t kInitialBuffer = 8 * 1024;
}

BufReader::BufReader(SocketPtr socket) : socket_(std::move(socket)), buf_(kInitialBuffer) {}

BufReader::BufReader(std::string data) : buf_(data.begin(), data.end()), end_(data.size()) {
  if (buf_.empty()) buf_.resize(1);
}

Task<bool> BufReader::fill(double deadline) {
  if (eof_) co_return false;
  if (!socket_) {
    eof_ = true;
    co_return false;
  }
  if (start_ == end_) start_ = end_ = 0;
  if (end_ == buf_.size()) {
    if (start_ > 0) {
      std::memmove(buf_.data(), buf_.data() + start_, end_ - start_);
      end_ -= start_;
      start_ = 0;
    } else {
      buf_.resize(buf_.size() * 2);
    }
  }
  size_t n = co_await socket_->read_some(buf_.data() + end_, buf_.size() - end_, deadline);
  if (n == 0) {
    eof_ = true;
    co_return false;
  }
  end_ += n;
  co_return true;
}

Task<std::string> BufReader::read_line(size_t max_length, double deadline) {
  size_t scanned = start_;
  for (;;) {
    const char* base = buf_.data();
    const void* nl = std::memchr(base + scanned, '\n', end_ - scanned);
    if (nl != nullptr) {
      size_t stop = static_cast<size_t>(static_cast<const char*>(nl) - base) + 1;
      if (stop - start_ > max_length) throw LineTooLong();
      std::string line(base + start_, stop - start_);
      start_ = stop;
      co_return line;
    }
    if (end_ - start_ > max_length) throw LineTooLong();
    size_t offset = end_ - start_;  // fill() may compact the buffer
    bool more = co_await fill(deadline);
    if (!more) {
      std::string rest(buf_.data() + start_, end_ - start_);
      start_ = end_;
      co_return rest;
    }
    scanned = start_ + offset;
  }
}

Task<size_t> BufReader::read_into(char* dst, size_t max, double deadline) {
  if (max == 0) co_return 0;
  if (start_ < end_) {
    size_t n = std::min(max, end_ - start_);
    std::memcpy(dst, buf_.data() + start_, n);
    start_ += n;
    co_return n;
  }
  if (eof_ || !socket_) {
    eof_ = true;
    co_return 0;
  }
  // Bypass the buffer for bulk reads: bodies go straight to the caller.
  size_t n = co_await socket_->read_some(dst, max, deadline);
  if (n == 0) eof_ = true;
  co_return n;
}

Task<std::string> BufReader::read_exact(size_t n, double deadline) {
  std::string out(n, '\0');
  size_t got = 0;
  while (got < n) {
    size_t k = co_await read_into(out.data() + got, n - got, deadline);
    if (k == 0) throw IncompleteRead();
    got += k;
  }
  co_return out;
}

}  // namespace jdblance
