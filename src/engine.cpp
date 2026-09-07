#include "kvc/engine.hpp"

#include "kvc/logging.hpp"

namespace kvc {

TransferEngine::TransferEngine(EngineConfig config, std::shared_ptr<Transport> transport)
    : config_(std::move(config)), transport_(std::move(transport)) {}

TransferEngine::~TransferEngine() { stop(); }

Status TransferEngine::start() {
  if (!transport_) return Status::InvalidArgument("transport is null");
  KVC_RETURN_IF_ERROR(config_.layout.validate());
  if (config_.self.empty()) {
    config_.self = transport_->self();
  } else if (config_.self != transport_->self()) {
    return Status::InvalidArgument("engine self '" + config_.self + "' differs from transport self '" +
                                   transport_->self() + "'");
  }
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (started_) return Status::Ok();
  }
  KVC_RETURN_IF_ERROR(transport_->start());
  std::lock_guard<std::mutex> lock(mu_);
  started_ = true;
  KVC_LOG_INFO << "engine '" << config_.self << "' started with " << config_.layout.to_string();
  return Status::Ok();
}

void TransferEngine::stop() {
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (!started_) return;
    started_ = false;
  }
  transport_->stop();
}

Result<RegionId> TransferEngine::register_kv_cache(void* base, uint64_t length, MemoryKind kind,
                                                   int device) {
  const uint64_t needed = config_.layout.total_bytes();
  if (length < needed) {
    return Status::InvalidArgument("region of " + std::to_string(length) +
                                   " bytes cannot hold a KV cache of " + std::to_string(needed) +
                                   " bytes (" + config_.layout.to_string() + ")");
  }
  return register_region(base, length, kind, device);
}

Result<RegionId> TransferEngine::register_region(void* base, uint64_t length, MemoryKind kind,
                                                 int device) {
  auto id = regions_.add(base, length, kind, device);
  if (!id.ok()) return id.status();
  const Status st = transport_->register_memory(*regions_.get(*id));
  if (!st.ok()) {
    regions_.remove(*id);
    return st;
  }
  return *id;
}

Status TransferEngine::unregister_region(RegionId id) {
  KVC_RETURN_IF_ERROR(regions_.remove(id));
  return transport_->deregister_memory(id);
}

std::optional<MemoryRegion> TransferEngine::region(RegionId id) const { return regions_.get(id); }

Status TransferEngine::connect(const PeerId& peer, const Endpoint& endpoint) {
  return transport_->connect(peer, endpoint);
}

Status TransferEngine::disconnect(const PeerId& peer) { return transport_->disconnect(peer); }

bool TransferEngine::is_connected(const PeerId& peer) const { return transport_->is_connected(peer); }

void TransferEngine::set_peer_layout(const PeerId& peer, KvLayout layout) {
  std::lock_guard<std::mutex> lock(mu_);
  peer_layouts_[peer] = layout;
}

KvLayout TransferEngine::peer_layout(const PeerId& peer) const {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = peer_layouts_.find(peer);
  return it == peer_layouts_.end() ? config_.layout : it->second;
}

TransferHandle TransferEngine::push_blocks(const PeerId& dst_peer, RegionId src_region,
                                           std::span<const BlockId> src_blocks, RegionId dst_region,
                                           std::span<const BlockId> dst_blocks, LayerRange range,
                                           TransferOptions opts) {
  if (!regions_.get(src_region)) {
    return TransferHandle::failed(
        Status::NotFound("source region " + std::to_string(src_region) + " not registered"),
        dst_peer);
  }
  auto descs = build_block_descriptors(config_.layout, src_region, src_blocks,
                                       peer_layout(dst_peer), dst_region, dst_blocks, range);
  if (!descs.ok()) return TransferHandle::failed(descs.status(), dst_peer);
  return push_raw(dst_peer, std::move(descs.value()), opts);
}

TransferHandle TransferEngine::pull_blocks(const PeerId& src_peer, RegionId src_region,
                                           std::span<const BlockId> src_blocks, RegionId dst_region,
                                           std::span<const BlockId> dst_blocks, LayerRange range,
                                           TransferOptions opts) {
  if (!regions_.get(dst_region)) {
    return TransferHandle::failed(
        Status::NotFound("destination region " + std::to_string(dst_region) + " not registered"),
        src_peer);
  }
  auto descs = build_block_descriptors(peer_layout(src_peer), src_region, src_blocks,
                                       config_.layout, dst_region, dst_blocks, range);
  if (!descs.ok()) return TransferHandle::failed(descs.status(), src_peer);
  return pull_raw(src_peer, std::move(descs.value()), opts);
}

TransferHandle TransferEngine::push_raw(const PeerId& dst_peer, std::vector<XferDesc> descs,
                                        TransferOptions opts) {
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (!started_) return TransferHandle::failed(Status::Unavailable("engine not started"), dst_peer);
  }
  metrics_.record_start();
  return track(transport_->write(dst_peer, std::move(descs), effective(opts)), true);
}

TransferHandle TransferEngine::pull_raw(const PeerId& src_peer, std::vector<XferDesc> descs,
                                        TransferOptions opts) {
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (!started_) return TransferHandle::failed(Status::Unavailable("engine not started"), src_peer);
  }
  metrics_.record_start();
  return track(transport_->read(src_peer, std::move(descs), effective(opts)), false);
}

TransferHandle TransferEngine::track(TransferHandle handle, bool push) {
  handle.on_complete([this, push](const TransferResult& r) { metrics_.record_done(r, push); });
  return handle;
}

TransferOptions TransferEngine::effective(TransferOptions opts) const {
  if (opts.timeout.count() <= 0) opts.timeout = config_.default_timeout;
  return opts;
}

Status TransferEngine::notify(const PeerId& peer, std::string_view payload) {
  const Status st = transport_->notify(peer, payload);
  if (st.ok()) metrics_.record_notify_sent();
  return st;
}

void TransferEngine::set_notify_handler(NotifyHandler handler) {
  if (!handler) {
    transport_->set_notify_handler(nullptr);
    return;
  }
  transport_->set_notify_handler(
      [this, handler = std::move(handler)](const PeerId& from, std::string_view payload) {
        metrics_.record_notify_received();
        handler(from, payload);
      });
}

void TransferEngine::set_disconnect_handler(DisconnectHandler handler) {
  transport_->set_disconnect_handler(std::move(handler));
}

}  // namespace kvc
