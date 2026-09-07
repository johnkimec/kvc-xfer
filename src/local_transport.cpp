#include "kvc/local_transport.hpp"

#include <cstring>

#include "kvc/layout.hpp"
#include "kvc/logging.hpp"

namespace kvc {

void LocalFabric::attach(const PeerId& peer, LocalTransport* transport) {
  std::lock_guard<std::mutex> lock(mu_);
  peers_[peer] = transport;
}

void LocalFabric::detach(const PeerId& peer) {
  std::lock_guard<std::mutex> lock(mu_);
  peers_.erase(peer);
}

LocalTransport* LocalFabric::find(const PeerId& peer) const {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = peers_.find(peer);
  return it == peers_.end() ? nullptr : it->second;
}

LocalTransport::LocalTransport(LocalTransportConfig config, std::shared_ptr<LocalFabric> fabric)
    : config_(std::move(config)), fabric_(std::move(fabric)) {}

LocalTransport::~LocalTransport() { stop(); }

Status LocalTransport::start() {
  if (config_.self.empty()) return Status::InvalidArgument("self peer id is empty");
  if (!fabric_) return Status::InvalidArgument("fabric is null");
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (started_) return Status::Ok();
    started_ = true;
  }
  if (!config_.synchronous) {
    std::lock_guard<std::mutex> lock(qmu_);
    worker_stop_ = false;
    worker_ = std::thread([this] { worker_loop(); });
  }
  fabric_->attach(config_.self, this);
  return Status::Ok();
}

void LocalTransport::stop() {
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (!started_) return;
    started_ = false;
  }
  fabric_->detach(config_.self);
  {
    std::lock_guard<std::mutex> lock(qmu_);
    worker_stop_ = true;
  }
  qcv_.notify_all();
  if (worker_.joinable()) worker_.join();
}

Status LocalTransport::connect(const PeerId& peer, const Endpoint& /*endpoint*/) {
  if (fabric_->find(peer) == nullptr) {
    return Status::Unavailable("peer '" + peer + "' is not attached to the local fabric");
  }
  std::lock_guard<std::mutex> lock(mu_);
  connected_[peer] = true;
  return Status::Ok();
}

Status LocalTransport::disconnect(const PeerId& peer) {
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (connected_.erase(peer) == 0) return Status::NotFound("not connected to '" + peer + "'");
  }
  DisconnectHandler handler;
  {
    std::shared_lock<std::shared_mutex> lock(handler_mu_);
    handler = disconnect_handler_;
    if (handler) handler(peer);
  }
  return Status::Ok();
}

bool LocalTransport::is_connected(const PeerId& peer) const {
  std::lock_guard<std::mutex> lock(mu_);
  return connected_.count(peer) != 0 && fabric_->find(peer) != nullptr;
}

Status LocalTransport::register_memory(const MemoryRegion& region) { return regions_.add(region); }

Status LocalTransport::deregister_memory(RegionId id) { return regions_.remove(id); }

void LocalTransport::run(std::function<void()> job) {
  if (config_.synchronous) {
    job();
    return;
  }
  bool inline_run = false;
  {
    std::lock_guard<std::mutex> lock(qmu_);
    if (worker_stop_) {
      inline_run = true;  // not started or already stopped: never leave work stranded
    } else {
      queue_.push_back(std::move(job));
    }
  }
  if (inline_run) {
    job();
    return;
  }
  qcv_.notify_one();
}

void LocalTransport::worker_loop() {
  while (true) {
    std::function<void()> job;
    {
      std::unique_lock<std::mutex> lock(qmu_);
      qcv_.wait(lock, [this] { return worker_stop_ || !queue_.empty(); });
      if (queue_.empty()) break;  // stop requested and drained
      job = std::move(queue_.front());
      queue_.pop_front();
    }
    job();
  }
}

Status LocalTransport::copy(const XferDesc& d, LocalTransport& src_side, LocalTransport& dst_side) {
  auto src = src_side.lookup(d.src_region);
  if (!src) {
    return Status::NotFound("region " + std::to_string(d.src_region) + " not registered on '" +
                            src_side.self() + "'");
  }
  auto dst = dst_side.lookup(d.dst_region);
  if (!dst) {
    return Status::NotFound("region " + std::to_string(d.dst_region) + " not registered on '" +
                            dst_side.self() + "'");
  }
  if (!src->contains(d.src_offset, d.length)) {
    return Status::OutOfRange("source range exceeds region " + std::to_string(d.src_region));
  }
  if (!dst->contains(d.dst_offset, d.length)) {
    return Status::OutOfRange("destination range exceeds region " + std::to_string(d.dst_region));
  }
  std::memcpy(dst->at(d.dst_offset), src->at(d.src_offset), d.length);
  return Status::Ok();
}

TransferHandle LocalTransport::write(const PeerId& dst, std::vector<XferDesc> descs,
                                     const TransferOptions& /*opts*/) {
  if (descs.empty()) return TransferHandle::failed(Status::InvalidArgument("no descriptors"), dst);
  if (!is_connected(dst)) {
    return TransferHandle::failed(Status::NotFound("not connected to peer '" + dst + "'"), dst);
  }
  LocalTransport* target = fabric_->find(dst);
  auto state = std::make_shared<TransferState>(next_id_.fetch_add(1), dst, total_bytes(descs),
                                               std::nullopt);
  run([this, target, descs = std::move(descs), state] {
    Status status;
    uint64_t bytes = 0;
    for (const auto& d : descs) {
      status = copy(d, *this, *target);
      if (!status.ok()) break;
      bytes += d.length;
    }
    state->complete(std::move(status), bytes);
  });
  return TransferHandle(state);
}

TransferHandle LocalTransport::read(const PeerId& src, std::vector<XferDesc> descs,
                                    const TransferOptions& /*opts*/) {
  if (descs.empty()) return TransferHandle::failed(Status::InvalidArgument("no descriptors"), src);
  if (!is_connected(src)) {
    return TransferHandle::failed(Status::NotFound("not connected to peer '" + src + "'"), src);
  }
  LocalTransport* target = fabric_->find(src);
  auto state = std::make_shared<TransferState>(next_id_.fetch_add(1), src, total_bytes(descs),
                                               std::nullopt);
  run([this, target, descs = std::move(descs), state] {
    Status status;
    uint64_t bytes = 0;
    for (const auto& d : descs) {
      status = copy(d, *target, *this);
      if (!status.ok()) break;
      bytes += d.length;
    }
    state->complete(std::move(status), bytes);
  });
  return TransferHandle(state);
}

Status LocalTransport::notify(const PeerId& dst, std::string_view payload) {
  if (!is_connected(dst)) return Status::NotFound("not connected to peer '" + dst + "'");
  LocalTransport* target = fabric_->find(dst);
  run([target, from = config_.self, payload = std::string(payload)]() mutable {
    target->deliver_notify(from, std::move(payload));
  });
  return Status::Ok();
}

void LocalTransport::deliver_notify(const PeerId& from, std::string payload) {
  std::shared_lock<std::shared_mutex> lock(handler_mu_);
  if (notify_handler_) notify_handler_(from, payload);
}

void LocalTransport::set_notify_handler(NotifyHandler handler) {
  std::unique_lock<std::shared_mutex> lock(handler_mu_);
  notify_handler_ = std::move(handler);
}

void LocalTransport::set_disconnect_handler(DisconnectHandler handler) {
  std::unique_lock<std::shared_mutex> lock(handler_mu_);
  disconnect_handler_ = std::move(handler);
}

}  // namespace kvc
