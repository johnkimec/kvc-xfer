#include "kvc/transfer.hpp"

#include <atomic>

namespace kvc {

TransferState::TransferState(TransferId id, PeerId peer, uint64_t expected_bytes,
                             std::optional<Clock::time_point> deadline)
    : id_(id),
      peer_(std::move(peer)),
      expected_bytes_(expected_bytes),
      start_(Clock::now()),
      deadline_(deadline) {}

bool TransferState::complete(Status status, uint64_t bytes) {
  std::vector<Callback> callbacks;
  TransferResult result;
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (done_) return false;
    done_ = true;
    result_.status = std::move(status);
    result_.bytes = bytes;
    result_.elapsed = Clock::now() - start_;
    result = result_;
    callbacks.swap(callbacks_);
    cancel_hook_ = nullptr;
  }
  cv_.notify_all();
  for (auto& cb : callbacks) cb(result);
  return true;
}

bool TransferState::done() const {
  std::lock_guard<std::mutex> lock(mu_);
  return done_;
}

TransferResult TransferState::wait() const {
  std::unique_lock<std::mutex> lock(mu_);
  cv_.wait(lock, [this] { return done_; });
  return result_;
}

std::optional<TransferResult> TransferState::wait_for(std::chrono::milliseconds timeout) const {
  std::unique_lock<std::mutex> lock(mu_);
  if (!cv_.wait_for(lock, timeout, [this] { return done_; })) return std::nullopt;
  return result_;
}

void TransferState::on_complete(Callback cb) {
  TransferResult result;
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (!done_) {
      callbacks_.push_back(std::move(cb));
      return;
    }
    result = result_;
  }
  cb(result);
}

void TransferState::set_cancel_hook(std::function<void()> hook) {
  std::lock_guard<std::mutex> lock(mu_);
  if (!done_) cancel_hook_ = std::move(hook);
}

void TransferState::cancel() {
  std::function<void()> hook;
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (done_) return;
    hook = std::move(cancel_hook_);
    cancel_hook_ = nullptr;
  }
  if (hook) hook();
  complete(Status::Cancelled("cancelled by caller"), 0);
}

TransferHandle TransferHandle::failed(Status status, PeerId peer) {
  static std::atomic<TransferId> counter{1};
  // High bit marks synthetic ids so they never collide with transport ids.
  const TransferId id = (1ull << 63) | counter.fetch_add(1);
  auto state = std::make_shared<TransferState>(id, std::move(peer), 0, std::nullopt);
  state->complete(std::move(status), 0);
  return TransferHandle(std::move(state));
}

const PeerId& TransferHandle::peer() const {
  static const PeerId kEmpty;
  return state_ ? state_->peer() : kEmpty;
}

TransferResult TransferHandle::wait() const {
  if (!state_) return TransferResult{Status::Internal("invalid transfer handle"), 0, {}};
  return state_->wait();
}

std::optional<TransferResult> TransferHandle::wait_for(std::chrono::milliseconds timeout) const {
  if (!state_) return TransferResult{Status::Internal("invalid transfer handle"), 0, {}};
  return state_->wait_for(timeout);
}

void TransferHandle::on_complete(TransferState::Callback cb) const {
  if (!state_) {
    cb(TransferResult{Status::Internal("invalid transfer handle"), 0, {}});
    return;
  }
  state_->on_complete(std::move(cb));
}

void TransferHandle::cancel() const {
  if (state_) state_->cancel();
}

}  // namespace kvc
