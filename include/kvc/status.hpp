#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <utility>

namespace kvc {

enum class StatusCode : uint32_t {
  kOk = 0,
  kInvalidArgument = 1,
  kNotFound = 2,
  kAlreadyExists = 3,
  kOutOfRange = 4,
  kUnavailable = 5,
  kTimeout = 6,
  kCancelled = 7,
  kDisconnected = 8,
  kProtocolError = 9,
  kUnimplemented = 10,
  kInternal = 11,
};

const char* status_code_name(StatusCode code);

// Lightweight error type. An Ok status carries no message.
class Status {
 public:
  Status() = default;
  Status(StatusCode code, std::string message) : code_(code), message_(std::move(message)) {}

  static Status Ok() { return Status(); }
  static Status InvalidArgument(std::string m) { return {StatusCode::kInvalidArgument, std::move(m)}; }
  static Status NotFound(std::string m) { return {StatusCode::kNotFound, std::move(m)}; }
  static Status AlreadyExists(std::string m) { return {StatusCode::kAlreadyExists, std::move(m)}; }
  static Status OutOfRange(std::string m) { return {StatusCode::kOutOfRange, std::move(m)}; }
  static Status Unavailable(std::string m) { return {StatusCode::kUnavailable, std::move(m)}; }
  static Status Timeout(std::string m) { return {StatusCode::kTimeout, std::move(m)}; }
  static Status Cancelled(std::string m) { return {StatusCode::kCancelled, std::move(m)}; }
  static Status Disconnected(std::string m) { return {StatusCode::kDisconnected, std::move(m)}; }
  static Status ProtocolError(std::string m) { return {StatusCode::kProtocolError, std::move(m)}; }
  static Status Unimplemented(std::string m) { return {StatusCode::kUnimplemented, std::move(m)}; }
  static Status Internal(std::string m) { return {StatusCode::kInternal, std::move(m)}; }

  bool ok() const { return code_ == StatusCode::kOk; }
  StatusCode code() const { return code_; }
  const std::string& message() const { return message_; }
  std::string to_string() const;

  bool operator==(const Status& o) const { return code_ == o.code_ && message_ == o.message_; }
  bool operator!=(const Status& o) const { return !(*this == o); }

 private:
  StatusCode code_ = StatusCode::kOk;
  std::string message_;
};

// Value-or-Status. Constructing from an Ok status without a value is a bug and
// is converted into an Internal error rather than left ambiguous.
template <typename T>
class Result {
 public:
  Result(T value) : value_(std::move(value)) {}  // NOLINT(google-explicit-constructor)
  Result(Status status) : status_(std::move(status)) {  // NOLINT(google-explicit-constructor)
    if (status_.ok()) status_ = Status::Internal("Result built from Ok status without a value");
  }

  bool ok() const { return value_.has_value(); }
  const Status& status() const { return status_; }

  T& value() & { return *value_; }
  const T& value() const& { return *value_; }
  T&& value() && { return std::move(*value_); }

  T* operator->() { return &*value_; }
  const T* operator->() const { return &*value_; }
  T& operator*() { return *value_; }
  const T& operator*() const { return *value_; }

 private:
  std::optional<T> value_;
  Status status_;
};

}  // namespace kvc

#define KVC_RETURN_IF_ERROR(expr)              \
  do {                                         \
    ::kvc::Status _kvc_status = (expr);        \
    if (!_kvc_status.ok()) return _kvc_status; \
  } while (0)
