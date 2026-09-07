#pragma once

#include <condition_variable>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>

#include "kvc/memory.hpp"
#include "kvc/transport.hpp"

namespace kvc {

class LocalTransport;

// In-process switchboard connecting LocalTransport instances by PeerId. Used
// for tests and for co-located prefill/decode workers sharing an address
// space. Transports must outlive their attachment (they detach in stop()).
class LocalFabric {
 public:
  void attach(const PeerId& peer, LocalTransport* transport);
  void detach(const PeerId& peer);
  LocalTransport* find(const PeerId& peer) const;

 private:
  mutable std::mutex mu_;
  std::map<PeerId, LocalTransport*> peers_;
};

struct LocalTransportConfig {
  PeerId self;
  // When true, writes/reads/notifies complete on the calling thread. When
  // false a worker thread executes them, mimicking real asynchronous
  // transports (default).
  bool synchronous = false;
};

// memcpy-based transport. Peers connect by name; endpoints are ignored.
class LocalTransport : public Transport {
 public:
  LocalTransport(LocalTransportConfig config, std::shared_ptr<LocalFabric> fabric);
  ~LocalTransport() override;

  std::string scheme() const override { return "local"; }
  const PeerId& self() const override { return config_.self; }

  Status start() override;
  void stop() override;

  Status connect(const PeerId& peer, const Endpoint& endpoint) override;
  Status disconnect(const PeerId& peer) override;
  bool is_connected(const PeerId& peer) const override;

  Status register_memory(const MemoryRegion& region) override;
  Status deregister_memory(RegionId id) override;

  TransferHandle write(const PeerId& dst, std::vector<XferDesc> descs,
                       const TransferOptions& opts = {}) override;
  TransferHandle read(const PeerId& src, std::vector<XferDesc> descs,
                      const TransferOptions& opts = {}) override;

  Status notify(const PeerId& dst, std::string_view payload) override;
  void set_notify_handler(NotifyHandler handler) override;
  void set_disconnect_handler(DisconnectHandler handler) override;

 private:
  friend class LocalFabric;

  std::optional<MemoryRegion> lookup(RegionId id) const { return regions_.get(id); }
  void deliver_notify(const PeerId& from, std::string payload);
  void run(std::function<void()> job);
  void worker_loop();
  Status copy(const XferDesc& d, LocalTransport& src_side, LocalTransport& dst_side);

  LocalTransportConfig config_;
  std::shared_ptr<LocalFabric> fabric_;
  MemoryRegistry regions_;

  mutable std::mutex mu_;
  std::map<PeerId, bool> connected_;
  bool started_ = false;
  mutable std::shared_mutex handler_mu_;  // shared while a handler runs
  NotifyHandler notify_handler_;
  DisconnectHandler disconnect_handler_;
  std::atomic<TransferId> next_id_{1};

  std::mutex qmu_;
  std::condition_variable qcv_;
  std::deque<std::function<void()>> queue_;
  bool worker_stop_ = true;  // false only while the worker thread is live
  std::thread worker_;
};

}  // namespace kvc
