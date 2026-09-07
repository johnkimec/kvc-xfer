#include "kvc/status.hpp"

#include "kvc/types.hpp"

namespace kvc {

const char* status_code_name(StatusCode code) {
  switch (code) {
    case StatusCode::kOk: return "OK";
    case StatusCode::kInvalidArgument: return "INVALID_ARGUMENT";
    case StatusCode::kNotFound: return "NOT_FOUND";
    case StatusCode::kAlreadyExists: return "ALREADY_EXISTS";
    case StatusCode::kOutOfRange: return "OUT_OF_RANGE";
    case StatusCode::kUnavailable: return "UNAVAILABLE";
    case StatusCode::kTimeout: return "TIMEOUT";
    case StatusCode::kCancelled: return "CANCELLED";
    case StatusCode::kDisconnected: return "DISCONNECTED";
    case StatusCode::kProtocolError: return "PROTOCOL_ERROR";
    case StatusCode::kUnimplemented: return "UNIMPLEMENTED";
    case StatusCode::kInternal: return "INTERNAL";
  }
  return "UNKNOWN";
}

std::string Status::to_string() const {
  if (ok()) return "OK";
  std::string s = status_code_name(code_);
  if (!message_.empty()) {
    s += ": ";
    s += message_;
  }
  return s;
}

const char* memory_kind_name(MemoryKind kind) {
  switch (kind) {
    case MemoryKind::kHost: return "host";
    case MemoryKind::kHostPinned: return "host_pinned";
    case MemoryKind::kDevice: return "device";
  }
  return "unknown";
}

std::string Endpoint::to_string() const {
  if (host.find(':') != std::string::npos) return "[" + host + "]:" + std::to_string(port);
  return host + ":" + std::to_string(port);
}

std::optional<Endpoint> Endpoint::parse(std::string_view text) {
  if (text.empty()) return std::nullopt;
  std::string_view host_part;
  std::string_view port_part;
  if (text.front() == '[') {
    auto close = text.find(']');
    if (close == std::string_view::npos || close + 1 >= text.size() || text[close + 1] != ':') {
      return std::nullopt;
    }
    host_part = text.substr(1, close - 1);
    port_part = text.substr(close + 2);
  } else {
    auto colon = text.rfind(':');
    if (colon == std::string_view::npos || colon == 0) return std::nullopt;
    host_part = text.substr(0, colon);
    port_part = text.substr(colon + 1);
  }
  if (port_part.empty() || port_part.size() > 5) return std::nullopt;
  uint32_t port = 0;
  for (char c : port_part) {
    if (c < '0' || c > '9') return std::nullopt;
    port = port * 10 + static_cast<uint32_t>(c - '0');
  }
  if (port > 65535) return std::nullopt;
  return Endpoint{std::string(host_part), static_cast<uint16_t>(port)};
}

}  // namespace kvc
