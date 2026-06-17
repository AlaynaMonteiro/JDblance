#include "log.hpp"

#include <unistd.h>

#include <cerrno>
#include <ctime>

namespace jdblance::log {

namespace {
Level g_level = Level::Info;
std::string g_access;
constexpr size_t kAccessFlushBytes = 64 * 1024;

void write_fd(int fd, std::string_view data) noexcept {
  while (!data.empty()) {
    ssize_t n = ::write(fd, data.data(), data.size());
    if (n < 0) {
      if (errno == EINTR) continue;
      return;   // nowhere left to report a logging failure
    }
    data.remove_prefix(static_cast<size_t>(n));
  }
}

const char* level_name(Level level) noexcept {
  switch (level) {
    case Level::Debug: return "DEBUG";
    case Level::Info: return "INFO ";
    case Level::Warning: return "WARN ";
    case Level::Error: return "ERROR";
  }
  return "?";
}
}  // namespace

void set_level(Level level) noexcept { g_level = level; }

Level parse_level(std::string_view name) noexcept {
  if (name == "debug") return Level::Debug;
  if (name == "warning" || name == "warn") return Level::Warning;
  if (name == "error") return Level::Error;
  return Level::Info;
}

bool enabled(Level level) noexcept { return level >= g_level; }

void write(Level level, std::string_view message) {
  if (!enabled(level)) return;
  std::time_t now = std::time(nullptr);
  std::tm tm{};
  localtime_r(&now, &tm);
  char stamp[16];
  std::strftime(stamp, sizeof stamp, "%H:%M:%S", &tm);
  std::string line;
  line.reserve(message.size() + 32);
  line += stamp;
  line += ' ';
  line += level_name(level);
  line += " jdblance: ";
  line += message;
  line += '\n';
  flush();   // keep ordering with buffered access lines
  write_fd(STDOUT_FILENO, line);
}

void access(std::string_view json_line) {
  g_access.append(json_line);
  g_access.push_back('\n');
  if (g_access.size() >= kAccessFlushBytes) flush();
}

void flush() noexcept {
  if (g_access.empty()) return;
  write_fd(STDOUT_FILENO, g_access);
  g_access.clear();
}

}  // namespace jdblance::log
