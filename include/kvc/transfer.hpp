#pragma once

#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include "kvc/status.hpp"
#include "kvc/types.hpp"

namespace kvc {

struct TransferResult {
  Status status;
  uint64_t bytes = 0;                    // bytes actually landed
  std::chrono::nanoseconds elapsed{0};   // submit -> completion
  bool ok() const { return status.ok(); }
};

// Shared completion state for one in-flight transfer. Transports own the
// lifecycle; users interact through TransferHandle.
class TransferState {
 public:
  using Callback = std::function<void(const TransferResult&)>;
  using Clock = std::chrono::steady_clock;

  TransferState(TransferId id, PeerId peer, uint64_t expected_bytes,
                std::optional<Clock::time_point> deadline);

  TransferId id() const { return id_; }
  const PeerId& peer() const { return peer_; }
  uint64_t expected_bytes() const { return expected_bytes_; }
  Clock::time_point start() const { return start_; }
  std::optional<Clock::time_point> deadline() const { return deadline_; }

  // First call wins; later calls are ignored. Callbacks run on the caller's
  // thread, outside the lock. Returns true if this call completed it.
  bool complete(Status status, uint64_t bytes);

  bool done() const;
  TransferResult wait() const;
  std::optional<TransferResult> wait_for(std::chrono::milliseconds timeout) const;

  // Fires immediately if already done.
  void on_complete(Callback cb);

  void set_cancel_hook(std::function<void()> hook);
  void cancel();

 private:
  const TransferId id_;
  const PeerId peer_;
  const uint64_t expected_bytes_;
  const Clock::time_point start_;
  const std::optional<Clock::time_point> deadline_;

  mutable std::mutex mu_;
  mutable std::condition_variable cv_;
  bool done_ = false;
  TransferResult result_;
  std::vector<Callback> callbacks_;
  std::function<void()> cancel_hook_;
};

// Cheap, copyable reference to a transfer. A default-constructed handle is
// invalid; failed() produces an already-completed handle.
class TransferHandle {
 public:
  TransferHandle() = default;
  explicit TransferHandle(std::shared_ptr<TransferState> state) : state_(std::move(state)) {}

  static TransferHandle failed(Status status, PeerId peer = {});

  bool valid() const { return state_ != nullptr; }
  TransferId id() const { return state_ ? state_->id() : kInvalidTransfer; }
  const PeerId& peer() const;
  uint64_t expected_bytes() const { return state_ ? state_->expected_bytes() : 0; }

  bool done() const { return !state_ || state_->done(); }
  TransferResult wait() const;
  std::optional<TransferResult> wait_for(std::chrono::milliseconds timeout) const;
  void on_complete(TransferState::Callback cb) const;
  void cancel() const;

  std::shared_ptr<TransferState> state() const { return state_; }

 private:
  std::shared_ptr<TransferState> state_;
};

}  // namespace kvc
