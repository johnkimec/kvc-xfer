#include "kvc/handoff.hpp"

#include "kvc/logging.hpp"
#include "kvc/protocol.hpp"

namespace kvc {

namespace {

constexpr uint8_t kMagic0 = 0xC4;
constexpr uint8_t kMagic1 = 0x48;  // 'H'
constexpr uint32_t kMaxBlocksPerRequest = 1u << 24;

enum class MsgKind : uint8_t { kPrepare = 1, kReady = 2, kAbort = 3 };

bool is_handoff_message(std::string_view p) {
  return p.size() >= 3 && static_cast<uint8_t>(p[0]) == kMagic0 &&
         static_cast<uint8_t>(p[1]) == kMagic1;
}

wire::Writer begin_message(MsgKind kind) {
  wire::Writer w;
  w.u8(kMagic0).u8(kMagic1).u8(static_cast<uint8_t>(kind));
  return w;
}

std::string_view as_view(const wire::Writer& w) {
  return {reinterpret_cast<const char*>(w.data().data()), w.size()};
}

Status status_from_wire(uint32_t code, std::string message) {
  if (code == 0) return Status::Ok();
  if (code > static_cast<uint32_t>(StatusCode::kInternal)) {
    return Status::Internal("unknown remote status code " + std::to_string(code) + ": " + message);
  }
  return Status(static_cast<StatusCode>(code), std::move(message));
}

}  // namespace

KvHandoff::KvHandoff(TransferEngine& engine, HandoffConfig config)
    : engine_(engine), config_(config) {
  engine_.set_notify_handler(
      [this](const PeerId& from, std::string_view payload) { on_notify(from, payload); });
  engine_.set_disconnect_handler([this](const PeerId& peer) { on_disconnect(peer); });
  reaper_ = std::thread([this] { reaper_loop(); });
}

KvHandoff::~KvHandoff() {
  alive_.reset();  // late transfer callbacks become no-ops
  {
    std::lock_guard<std::mutex> lock(reaper_mu_);
    stop_ = true;
  }
  reaper_cv_.notify_all();
  if (reaper_.joinable()) reaper_.join();
  // Clearing the handlers blocks until in-flight handler invocations return.
  engine_.set_notify_handler(nullptr);
  engine_.set_disconnect_handler(nullptr);

  // Outstanding publish handles are completed so nobody blocks on them.
  // Outstanding expects are dropped *without* invoking their callbacks: the
  // owner is tearing down and the callback's captures may already be gone.
  std::map<std::string, PendingPublish> publishes;
  {
    std::lock_guard<std::mutex> lock(mu_);
    expects_.clear();
    publishes.swap(pending_publishes_);
    prepares_.clear();
  }
  const Status why = Status::Cancelled("handoff destroyed");
  for (auto& [_, p] : publishes) p.state->complete(why, 0);
}

std::shared_ptr<TransferState> KvHandoff::make_state(const PeerId& peer, uint64_t bytes) {
  // Distinct id space from transport ids: bit 62 set.
  const TransferId id = (1ull << 62) | next_id_.fetch_add(1);
  return std::make_shared<TransferState>(id, peer, bytes, std::nullopt);
}

// ---------------------------------------------------------------------------
// Consumer side
// ---------------------------------------------------------------------------

Status KvHandoff::expect(const std::string& request_id, const PeerId& producer,
                         RegionId dst_region, std::vector<BlockId> dst_blocks,
                         ReadyCallback on_ready, LayerRange range) {
  if (request_id.empty()) return Status::InvalidArgument("request_id is empty");
  if (producer.empty()) return Status::InvalidArgument("producer peer id is empty");
  if (dst_blocks.empty()) return Status::InvalidArgument("no destination blocks");
  if (dst_blocks.size() > kMaxBlocksPerRequest) return Status::InvalidArgument("too many blocks");
  if (!engine_.region(dst_region)) {
    return Status::NotFound("destination region " + std::to_string(dst_region) +
                            " not registered");
  }
  const KvLayout& layout = engine_.layout();
  for (BlockId b : dst_blocks) {
    if (b >= layout.num_blocks) {
      return Status::OutOfRange("destination block " + std::to_string(b) + " >= num_blocks " +
                                std::to_string(layout.num_blocks));
    }
  }
  const LayerRange r = range.resolve(layout.num_layers);
  if (r.begin >= r.end || r.end > layout.num_layers) {
    return Status::InvalidArgument("invalid layer range for " + std::to_string(layout.num_layers) +
                                   " layers");
  }

  Expect e;
  e.producer = producer;
  e.dst_region = dst_region;
  e.blocks = dst_blocks;
  e.range = r;
  e.layer_done.assign(r.count(), false);
  e.layers_remaining = r.count();
  e.on_ready = std::move(on_ready);
  if (config_.ready_wait_timeout.count() > 0) {
    e.deadline = Clock::now() + config_.ready_wait_timeout;
  }
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (expects_.count(request_id) != 0) {
      return Status::AlreadyExists("request '" + request_id + "' is already expected");
    }
    expects_[request_id] = std::move(e);
  }

  wire::Writer w = begin_message(MsgKind::kPrepare);
  w.str(request_id).u32(dst_region).u32(r.begin).u32(r.end).u64(layout.fingerprint());
  w.u32(static_cast<uint32_t>(dst_blocks.size()));
  for (BlockId b : dst_blocks) w.u32(b);
  const Status sent = engine_.notify(producer, as_view(w));
  if (!sent.ok()) {
    std::lock_guard<std::mutex> lock(mu_);
    expects_.erase(request_id);
  }
  return sent;
}

void KvHandoff::handle_ready(const PeerId& from, wire::Reader& r) {
  std::string request_id, msg;
  uint32_t begin = 0, end = 0, code = 0;
  uint64_t bytes = 0;
  if (!r.str(request_id) || !r.u32(begin) || !r.u32(end) || !r.u32(code) || !r.str(msg) ||
      !r.u64(bytes)) {
    KVC_LOG_WARN << "malformed READY from '" << from << "'";
    return;
  }
  ReadyCallback cb;
  Status result;
  uint64_t total = 0;
  bool fire = false;
  {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = expects_.find(request_id);
    if (it == expects_.end()) {
      KVC_LOG_DEBUG << "READY for unknown request '" << request_id << "' from '" << from << "'";
      return;
    }
    Expect& e = it->second;
    if (e.producer != from) {
      KVC_LOG_WARN << "READY for '" << request_id << "' from '" << from << "' but expected '"
                   << e.producer << "'";
      return;
    }
    if (code != 0) {
      fire = true;
      result = status_from_wire(code, std::move(msg));
      cb = std::move(e.on_ready);
      expects_.erase(it);
    } else {
      e.bytes += bytes;
      const uint32_t lo = std::max(begin, e.range.begin);
      const uint32_t hi = std::min(end, e.range.end);
      for (uint32_t layer = lo; layer < hi; ++layer) {
        const uint32_t idx = layer - e.range.begin;
        if (!e.layer_done[idx]) {
          e.layer_done[idx] = true;
          --e.layers_remaining;
        }
      }
      if (e.layers_remaining == 0) {
        fire = true;
        total = e.bytes;
        cb = std::move(e.on_ready);
        expects_.erase(it);
      }
    }
  }
  if (fire && cb) cb(request_id, result, total);
}

// ---------------------------------------------------------------------------
// Producer side
// ---------------------------------------------------------------------------

TransferHandle KvHandoff::publish(const std::string& request_id, const PeerId& consumer,
                                  RegionId src_region, std::vector<BlockId> src_blocks,
                                  LayerRange range) {
  if (request_id.empty()) return TransferHandle::failed(Status::InvalidArgument("request_id is empty"), consumer);
  if (consumer.empty()) return TransferHandle::failed(Status::InvalidArgument("consumer peer id is empty"), consumer);
  if (src_blocks.empty()) return TransferHandle::failed(Status::InvalidArgument("no source blocks"), consumer);
  if (!engine_.region(src_region)) {
    return TransferHandle::failed(
        Status::NotFound("source region " + std::to_string(src_region) + " not registered"),
        consumer);
  }
  auto state = make_state(consumer, 0);
  std::optional<PushPlan> plan;
  {
    std::lock_guard<std::mutex> lock(mu_);
    plan = plan_locked(request_id, consumer, src_region, std::move(src_blocks), range, state);
  }
  if (plan) execute(std::move(*plan));
  return TransferHandle(state);
}

std::optional<KvHandoff::PushPlan> KvHandoff::plan_locked(const std::string& request_id,
                                                          const PeerId& consumer,
                                                          RegionId src_region,
                                                          std::vector<BlockId> src_blocks,
                                                          LayerRange range,
                                                          std::shared_ptr<TransferState> state) {
  auto it = prepares_.find(request_id);
  if (it == prepares_.end()) {
    if (pending_publishes_.count(request_id) != 0) {
      state->complete(Status::AlreadyExists("a publish for '" + request_id +
                                            "' is already waiting for PREPARE"),
                      0);
      return std::nullopt;
    }
    PendingPublish pp;
    pp.consumer = consumer;
    pp.src_region = src_region;
    pp.blocks = std::move(src_blocks);
    pp.range = range;
    pp.state = state;
    pp.deadline = Clock::now() + config_.prepare_wait_timeout;
    pending_publishes_[request_id] = std::move(pp);
    KVC_LOG_DEBUG << "publish '" << request_id << "' waiting for PREPARE from '" << consumer << "'";
    return std::nullopt;
  }

  const Prepare& p = it->second;
  if (p.consumer != consumer) {
    state->complete(Status::InvalidArgument("PREPARE for '" + request_id + "' came from '" +
                                            p.consumer + "' but publish targets '" + consumer + "'"),
                    0);
    return std::nullopt;
  }
  if (p.blocks.size() != src_blocks.size()) {
    state->complete(Status::InvalidArgument("block count mismatch for '" + request_id +
                                            "': producer has " + std::to_string(src_blocks.size()) +
                                            ", consumer expects " + std::to_string(p.blocks.size())),
                    0);
    return std::nullopt;
  }
  const LayerRange r = range.resolve(engine_.layout().num_layers);
  if (r.count() == 0 || r.begin < p.range.begin || r.end > p.range.end) {
    state->complete(Status::InvalidArgument("layer range [" + std::to_string(r.begin) + ", " +
                                            std::to_string(r.end) +
                                            ") is outside the consumer's expected range [" +
                                            std::to_string(p.range.begin) + ", " +
                                            std::to_string(p.range.end) + ")"),
                    0);
    return std::nullopt;
  }
  const uint64_t expected_fp = engine_.peer_layout(consumer).fingerprint();
  if (expected_fp != p.fingerprint) {
    state->complete(Status::InvalidArgument("layout fingerprint mismatch with consumer '" + consumer +
                                            "'; call TransferEngine::set_peer_layout with the "
                                            "consumer's KvLayout"),
                    0);
    return std::nullopt;
  }
  PushPlan plan;
  plan.request_id = request_id;
  plan.consumer = consumer;
  plan.src_region = src_region;
  plan.src_blocks = std::move(src_blocks);
  plan.dst_region = p.dst_region;
  plan.dst_blocks = p.blocks;
  plan.range = r;
  plan.state = std::move(state);
  return plan;
}

void KvHandoff::execute(PushPlan plan) {
  TransferHandle handle =
      engine_.push_blocks(plan.consumer, plan.src_region, plan.src_blocks, plan.dst_region,
                          plan.dst_blocks, plan.range, config_.transfer_options);
  auto state = plan.state;
  handle.on_complete([this, token = std::weak_ptr<int>(alive_), state, req = plan.request_id,
                      consumer = plan.consumer, range = plan.range](const TransferResult& r) {
    auto keep = token.lock();
    if (!keep) {
      state->complete(Status::Cancelled("handoff destroyed"), 0);
      return;
    }
    if (r.ok()) {
      std::lock_guard<std::mutex> lock(mu_);
      auto it = prepares_.find(req);
      if (it != prepares_.end()) {
        Prepare& p = it->second;
        for (uint32_t layer = range.begin; layer < range.end; ++layer) {
          const uint32_t idx = layer - p.range.begin;
          if (!p.layer_done[idx]) {
            p.layer_done[idx] = true;
            --p.layers_remaining;
          }
        }
        if (p.layers_remaining == 0) prepares_.erase(it);
      }
    }
    send_ready(consumer, req, range, r.status, r.bytes);
    state->complete(r.status, r.bytes);
  });
}

void KvHandoff::send_ready(const PeerId& consumer, const std::string& request_id, LayerRange range,
                           const Status& status, uint64_t bytes) {
  wire::Writer w = begin_message(MsgKind::kReady);
  w.str(request_id).u32(range.begin).u32(range.end).u32(static_cast<uint32_t>(status.code()));
  w.str(status.message()).u64(bytes);
  const Status sent = engine_.notify(consumer, as_view(w));
  if (!sent.ok()) {
    KVC_LOG_WARN << "could not send READY for '" << request_id << "' to '" << consumer
                 << "': " << sent.to_string();
  }
}

void KvHandoff::handle_prepare(const PeerId& from, wire::Reader& r) {
  std::string request_id;
  uint32_t dst_region = 0, begin = 0, end = 0, n = 0;
  uint64_t fingerprint = 0;
  if (!r.str(request_id) || !r.u32(dst_region) || !r.u32(begin) || !r.u32(end) ||
      !r.u64(fingerprint) || !r.u32(n) || n == 0 || n > kMaxBlocksPerRequest ||
      r.remaining() < static_cast<size_t>(n) * 4) {
    KVC_LOG_WARN << "malformed PREPARE from '" << from << "'";
    return;
  }
  Prepare p;
  p.consumer = from;
  p.dst_region = dst_region;
  p.blocks.resize(n);
  for (uint32_t i = 0; i < n; ++i) r.u32(p.blocks[i]);
  p.range = LayerRange::of(begin, end);
  p.layer_done.assign(p.range.count(), false);
  p.layers_remaining = p.range.count();
  p.fingerprint = fingerprint;

  std::optional<PushPlan> plan;
  {
    std::lock_guard<std::mutex> lock(mu_);
    prepares_[request_id] = std::move(p);
    auto it = pending_publishes_.find(request_id);
    if (it != pending_publishes_.end()) {
      PendingPublish pp = std::move(it->second);
      pending_publishes_.erase(it);
      plan = plan_locked(request_id, pp.consumer, pp.src_region, std::move(pp.blocks), pp.range,
                         std::move(pp.state));
    }
  }
  if (plan) execute(std::move(*plan));
}

// ---------------------------------------------------------------------------
// Shared
// ---------------------------------------------------------------------------

Status KvHandoff::abort(const std::string& request_id, std::string reason) {
  std::vector<PeerId> peers;
  ReadyCallback cb;
  std::shared_ptr<TransferState> state;
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (auto it = expects_.find(request_id); it != expects_.end()) {
      peers.push_back(it->second.producer);
      cb = std::move(it->second.on_ready);
      expects_.erase(it);
    }
    if (auto it = prepares_.find(request_id); it != prepares_.end()) {
      peers.push_back(it->second.consumer);
      prepares_.erase(it);
    }
    if (auto it = pending_publishes_.find(request_id); it != pending_publishes_.end()) {
      peers.push_back(it->second.consumer);
      state = std::move(it->second.state);
      pending_publishes_.erase(it);
    }
  }
  if (peers.empty()) return Status::NotFound("no handoff state for '" + request_id + "'");
  const Status why = Status::Cancelled("aborted locally: " + reason);
  if (cb) cb(request_id, why, 0);
  if (state) state->complete(why, 0);

  wire::Writer w = begin_message(MsgKind::kAbort);
  w.str(request_id).str(reason);
  std::sort(peers.begin(), peers.end());
  peers.erase(std::unique(peers.begin(), peers.end()), peers.end());
  Status last;
  for (const auto& peer : peers) {
    const Status s = engine_.notify(peer, as_view(w));
    if (!s.ok()) last = s;
  }
  return last;
}

void KvHandoff::handle_abort(const PeerId& from, wire::Reader& r) {
  std::string request_id, reason;
  if (!r.str(request_id) || !r.str(reason)) {
    KVC_LOG_WARN << "malformed ABORT from '" << from << "'";
    return;
  }
  ReadyCallback cb;
  std::shared_ptr<TransferState> state;
  {
    std::lock_guard<std::mutex> lock(mu_);
    prepares_.erase(request_id);
    if (auto it = pending_publishes_.find(request_id); it != pending_publishes_.end()) {
      state = std::move(it->second.state);
      pending_publishes_.erase(it);
    }
    if (auto it = expects_.find(request_id); it != expects_.end()) {
      cb = std::move(it->second.on_ready);
      expects_.erase(it);
    }
  }
  const Status why = Status::Cancelled("aborted by '" + from + "': " + reason);
  if (cb) cb(request_id, why, 0);
  if (state) state->complete(why, 0);
}

void KvHandoff::on_notify(const PeerId& from, std::string_view payload) {
  if (!is_handoff_message(payload)) {
    NotifyHandler user;
    {
      std::lock_guard<std::mutex> lock(mu_);
      user = user_notify_;
    }
    if (user) user(from, payload);
    return;
  }
  wire::Reader r(payload);
  r.skip(2);
  uint8_t kind = 0;
  r.u8(kind);
  switch (static_cast<MsgKind>(kind)) {
    case MsgKind::kPrepare: handle_prepare(from, r); return;
    case MsgKind::kReady: handle_ready(from, r); return;
    case MsgKind::kAbort: handle_abort(from, r); return;
  }
  KVC_LOG_WARN << "unknown handoff message kind " << int{kind} << " from '" << from << "'";
}

void KvHandoff::on_disconnect(const PeerId& peer) {
  std::vector<std::pair<std::string, ReadyCallback>> callbacks;
  std::vector<std::shared_ptr<TransferState>> states;
  DisconnectHandler user;
  {
    std::lock_guard<std::mutex> lock(mu_);
    for (auto it = expects_.begin(); it != expects_.end();) {
      if (it->second.producer == peer) {
        callbacks.emplace_back(it->first, std::move(it->second.on_ready));
        it = expects_.erase(it);
      } else {
        ++it;
      }
    }
    for (auto it = prepares_.begin(); it != prepares_.end();) {
      it = it->second.consumer == peer ? prepares_.erase(it) : std::next(it);
    }
    for (auto it = pending_publishes_.begin(); it != pending_publishes_.end();) {
      if (it->second.consumer == peer) {
        states.push_back(std::move(it->second.state));
        it = pending_publishes_.erase(it);
      } else {
        ++it;
      }
    }
    user = user_disconnect_;
  }
  const Status why = Status::Disconnected("peer '" + peer + "' disconnected");
  for (auto& [id, cb] : callbacks) {
    if (cb) cb(id, why, 0);
  }
  for (auto& s : states) s->complete(why, 0);
  if (user) user(peer);
}

void KvHandoff::reaper_loop() {
  while (true) {
    {
      std::unique_lock<std::mutex> lock(reaper_mu_);
      if (reaper_cv_.wait_for(lock, std::chrono::milliseconds(50), [this] { return stop_; })) break;
    }
    const auto now = Clock::now();
    std::vector<std::pair<std::string, ReadyCallback>> expired_expects;
    std::vector<std::pair<std::string, PeerId>> aborts;
    std::vector<std::shared_ptr<TransferState>> expired_publishes;
    {
      std::lock_guard<std::mutex> lock(mu_);
      for (auto it = expects_.begin(); it != expects_.end();) {
        if (it->second.deadline && *it->second.deadline <= now) {
          expired_expects.emplace_back(it->first, std::move(it->second.on_ready));
          aborts.emplace_back(it->first, it->second.producer);
          it = expects_.erase(it);
        } else {
          ++it;
        }
      }
      for (auto it = pending_publishes_.begin(); it != pending_publishes_.end();) {
        if (it->second.deadline <= now) {
          expired_publishes.push_back(std::move(it->second.state));
          it = pending_publishes_.erase(it);
        } else {
          ++it;
        }
      }
    }
    for (auto& [id, cb] : expired_expects) {
      if (cb) {
        cb(id, Status::Timeout("no READY for '" + id + "' within " +
                               std::to_string(config_.ready_wait_timeout.count()) + "ms"),
           0);
      }
    }
    for (auto& [id, producer] : aborts) {
      wire::Writer w = begin_message(MsgKind::kAbort);
      w.str(id).str("consumer timed out waiting for READY");
      engine_.notify(producer, as_view(w));
    }
    for (auto& s : expired_publishes) {
      s->complete(Status::Timeout("no PREPARE from consumer within " +
                                  std::to_string(config_.prepare_wait_timeout.count()) + "ms"),
                  0);
    }
  }
}

bool KvHandoff::has_prepare(const std::string& request_id) const {
  std::lock_guard<std::mutex> lock(mu_);
  return prepares_.count(request_id) != 0;
}

size_t KvHandoff::pending_expects() const {
  std::lock_guard<std::mutex> lock(mu_);
  return expects_.size();
}

size_t KvHandoff::pending_publishes() const {
  std::lock_guard<std::mutex> lock(mu_);
  return pending_publishes_.size();
}

size_t KvHandoff::pending_prepares() const {
  std::lock_guard<std::mutex> lock(mu_);
  return prepares_.size();
}

void KvHandoff::set_notify_handler(NotifyHandler handler) {
  std::lock_guard<std::mutex> lock(mu_);
  user_notify_ = std::move(handler);
}

void KvHandoff::set_disconnect_handler(DisconnectHandler handler) {
  std::lock_guard<std::mutex> lock(mu_);
  user_disconnect_ = std::move(handler);
}

}  // namespace kvc
