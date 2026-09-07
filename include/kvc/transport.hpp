#pragma once

#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include "kvc/memory.hpp"
#include "kvc/status.hpp"
#include "kvc/transfer.hpp"
#include "kvc/types.hpp"

namespace kvc {

using NotifyHandler = std::function<void(const PeerId& from, std::string_view payload)>;
using DisconnectHandler = std::function<void(const PeerId& peer)>;

// Data-plane abstraction. Implementations move bytes between registered
// regions on different peers and deliver small ordered control messages.
//
// Threading: handlers and completion callbacks are invoked on transport
// threads and must not block for long. Calling back into the transport from a
// handler (e.g. to answer a notify with a write) is allowed.
class Transport {
 public:
  virtual ~Transport() = default;

  virtual std::string scheme() const = 0;
  virtual const PeerId& self() const = 0;

  virtual Status start() = 0;
  virtual void stop() = 0;

  virtual Status connect(const PeerId& peer, const Endpoint& endpoint) = 0;
  virtual Status disconnect(const PeerId& peer) = 0;
  virtual bool is_connected(const PeerId& peer) const = 0;

  // Makes a region addressable by peers under region.id.
  virtual Status register_memory(const MemoryRegion& region) = 0;
  virtual Status deregister_memory(RegionId id) = 0;

  // Push: local src regions -> remote dst regions. Completes once the remote
  // side has landed the bytes (or reports why it could not).
  virtual TransferHandle write(const PeerId& dst, std::vector<XferDesc> descs,
                               const TransferOptions& opts = {}) = 0;
  // Pull: remote src regions -> local dst regions.
  virtual TransferHandle read(const PeerId& src, std::vector<XferDesc> descs,
                              const TransferOptions& opts = {}) = 0;

  // Small opaque control message. Ordered with respect to writes issued to
  // the same peer from the same thread.
  virtual Status notify(const PeerId& dst, std::string_view payload) = 0;

  virtual void set_notify_handler(NotifyHandler handler) = 0;
  virtual void set_disconnect_handler(DisconnectHandler handler) = 0;
};

}  // namespace kvc
