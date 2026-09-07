#pragma once

#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "kvc/engine.hpp"
#include "kvc/protocol.hpp"

namespace kvc {

struct HandoffConfig {
  // Producer side: how long publish() may wait for the consumer's PREPARE.
  std::chrono::milliseconds prepare_wait_timeout{30000};
  // Consumer side: how long expect() waits for READY. 0 = no deadline.
  std::chrono::milliseconds ready_wait_timeout{0};
  TransferOptions transfer_options{};
};

// Prefill -> decode KV handoff protocol on top of TransferEngine.
//
//   consumer (decode)                       producer (prefill)
//   ---------------------------------------------------------
//   expect(req, blocks)  --PREPARE-->       (stored until publish)
//                                           publish(req, blocks)
//                        <--WRITE(s)--      push_blocks per layer range
//                        <--READY--         (per range; consumer aggregates)
//   on_ready(req)
//
// publish() may be called before PREPARE arrives; it is queued until the
// consumer's blocks are known or prepare_wait_timeout elapses. Layer-wise
// pipelining works by calling publish() several times with disjoint ranges;
// the consumer's callback fires once every layer in its expected range has
// landed. Either side may abort().
//
// Destroying a KvHandoff completes queued publish handles with kCancelled and
// drops pending expects without invoking their callbacks.
class KvHandoff {
 public:
  using ReadyCallback =
      std::function<void(const std::string& request_id, const Status& status, uint64_t bytes)>;

  explicit KvHandoff(TransferEngine& engine, HandoffConfig config = {});
  ~KvHandoff();

  KvHandoff(const KvHandoff&) = delete;
  KvHandoff& operator=(const KvHandoff&) = delete;

  // Consumer: announce where `request_id`'s KV must land.
  Status expect(const std::string& request_id, const PeerId& producer, RegionId dst_region,
                std::vector<BlockId> dst_blocks, ReadyCallback on_ready,
                LayerRange range = LayerRange::all());

  // Producer: push local blocks for `request_id` to `consumer`. The handle
  // completes when the bytes have landed and READY has been sent.
  TransferHandle publish(const std::string& request_id, const PeerId& consumer,
                         RegionId src_region, std::vector<BlockId> src_blocks,
                         LayerRange range = LayerRange::all());

  // Either side: drop all state for the request and tell the peer.
  Status abort(const std::string& request_id, std::string reason);

  bool has_prepare(const std::string& request_id) const;
  size_t pending_expects() const;
  size_t pending_publishes() const;
  size_t pending_prepares() const;

  // Non-handoff notifies and disconnects are forwarded to these.
  void set_notify_handler(NotifyHandler handler);
  void set_disconnect_handler(DisconnectHandler handler);

 private:
  using Clock = std::chrono::steady_clock;

  struct Expect {
    PeerId producer;
    RegionId dst_region;
    std::vector<BlockId> blocks;
    LayerRange range;  // resolved
    std::vector<bool> layer_done;
    uint32_t layers_remaining;
    uint64_t bytes = 0;
    ReadyCallback on_ready;
    std::optional<Clock::time_point> deadline;
  };
  struct Prepare {
    PeerId consumer;
    RegionId dst_region;
    std::vector<BlockId> blocks;
    LayerRange range;  // resolved
    std::vector<bool> layer_done;
    uint32_t layers_remaining;
    uint64_t fingerprint;
  };
  struct PendingPublish {
    PeerId consumer;
    RegionId src_region;
    std::vector<BlockId> blocks;
    LayerRange range;
    std::shared_ptr<TransferState> state;
    Clock::time_point deadline;
  };
  struct PushPlan {
    std::string request_id;
    PeerId consumer;
    RegionId src_region;
    std::vector<BlockId> src_blocks;
    RegionId dst_region;
    std::vector<BlockId> dst_blocks;
    LayerRange range;
    std::shared_ptr<TransferState> state;
  };

  void on_notify(const PeerId& from, std::string_view payload);
  void on_disconnect(const PeerId& peer);
  void handle_prepare(const PeerId& from, wire::Reader& r);
  void handle_ready(const PeerId& from, wire::Reader& r);
  void handle_abort(const PeerId& from, wire::Reader& r);

  // Pairs a publish with a stored PREPARE. Caller holds mu_. Returns a plan
  // to execute after the lock is released, or completes `state` with an error.
  std::optional<PushPlan> plan_locked(const std::string& request_id, const PeerId& consumer,
                                      RegionId src_region, std::vector<BlockId> src_blocks,
                                      LayerRange range, std::shared_ptr<TransferState> state);
  void execute(PushPlan plan);
  void send_ready(const PeerId& consumer, const std::string& request_id, LayerRange range,
                  const Status& status, uint64_t bytes);
  void reaper_loop();
  std::shared_ptr<TransferState> make_state(const PeerId& peer, uint64_t bytes);

  TransferEngine& engine_;
  HandoffConfig config_;

  mutable std::mutex mu_;
  std::map<std::string, Expect> expects_;
  std::map<std::string, Prepare> prepares_;
  std::map<std::string, PendingPublish> pending_publishes_;
  NotifyHandler user_notify_;
  DisconnectHandler user_disconnect_;
  std::atomic<uint64_t> next_id_{1};
  std::shared_ptr<int> alive_ = std::make_shared<int>(0);  // liveness token for callbacks

  std::mutex reaper_mu_;
  std::condition_variable reaper_cv_;
  bool stop_ = false;
  std::thread reaper_;
};

}  // namespace kvc
