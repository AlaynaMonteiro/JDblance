// Logging: human lines for operators, one JSON object per line for access logs.
#pragma once

#include <string>
#include <string_view>

namespace jdblance::log {

enum class Level { Debug = 0, Info = 1, Warning = 2, Error = 3 };

void set_level(Level level) noexcept;
Level parse_level(std::string_view name) noexcept;   // unknown -> Info
bool enabled(Level level) noexcept;

// "HH:MM:SS LEVEL jdblance: message", flushed immediately.
void write(Level level, std::string_view message);
inline void debug(std::string_view m) { write(Level::Debug, m); }
inline void info(std::string_view m) { write(Level::Info, m); }
inline void warning(std::string_view m) { write(Level::Warning, m); }
inline void error(std::string_view m) { write(Level::Error, m); }

// Access lines are buffered (one write per ~64 KiB or per flush()), because a
// write(2) per request costs measurable throughput at high rates.  The worker
// flushes on a short timer and at exit.
void access(std::string_view json_line);
void flush() noexcept;

}  // namespace jdblance::log
