#pragma once

#include <functional>
#include <sstream>
#include <string_view>

namespace kvc {

enum class LogLevel : int { kTrace = 0, kDebug = 1, kInfo = 2, kWarn = 3, kError = 4, kOff = 5 };

using LogSink = std::function<void(LogLevel level, std::string_view message)>;

void set_log_level(LogLevel level);
LogLevel log_level();
// Replaces the default stderr sink. Pass nullptr to restore it.
void set_log_sink(LogSink sink);
const char* log_level_name(LogLevel level);

namespace detail {
bool log_enabled(LogLevel level);
class LogLine {
 public:
  explicit LogLine(LogLevel level) : level_(level) {}
  ~LogLine();
  std::ostringstream& stream() { return stream_; }

 private:
  LogLevel level_;
  std::ostringstream stream_;
};
}  // namespace detail

}  // namespace kvc

#define KVC_LOG(level)                          \
  if (!::kvc::detail::log_enabled(level)) {     \
  } else                                        \
    ::kvc::detail::LogLine(level).stream()

#define KVC_LOG_TRACE KVC_LOG(::kvc::LogLevel::kTrace)
#define KVC_LOG_DEBUG KVC_LOG(::kvc::LogLevel::kDebug)
#define KVC_LOG_INFO KVC_LOG(::kvc::LogLevel::kInfo)
#define KVC_LOG_WARN KVC_LOG(::kvc::LogLevel::kWarn)
#define KVC_LOG_ERROR KVC_LOG(::kvc::LogLevel::kError)
