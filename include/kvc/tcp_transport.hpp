#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "kvc/memory.hpp"
#include "kvc/protocol.hpp"
#include "kvc/transport.hpp"

namespace kvc {

struct TcpTransportConfig {
  PeerId self;
  std::string bind_host = "0.0.0.0";
  uint16_t bind_port = 0;  // 0 = ephemeral; see local_endpoint()
  std::chrono::milliseconds connect_timeout{5000};
  bool tcp_nodelay = true;
  int socket_buffer_bytes = 0;  // 0 = leave kernel default
  // How often the reaper checks deadlines and joins finished connections.
  std::chrono::milliseconds reaper_interval{50};
};

// Stream-socket transport. One connection per peer carries interleaved data
// and control frames; payload bytes are scattered straight from/into
// registered regions with no intermediate copy. Host memory only.
class TcpTransport : public Transport {
 public:
  explicit TcpTransport(TcpTransportConfig config);
  ~TcpTransport() override;

  std::string scheme() const override { return "tcp"; }
  const PeerId& self() const override { return config_.self; }

  Status start() override;
  void stop() override;

  // Valid after start(); reports the bound port even if 0 was requested.
  Endpoint local_endpoint() const;

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

  size_t pending_transfers() const;

 private:
  struct Piece {
    const void* data;
    size_t len;
  };
  // One frame queued for sending. `meta` holds the header and any inline
  // metadata; `owned` holds chunk headers; `pieces` references both plus
  // user memory in submission order.
  struct OutFrame {
    std::vector<uint8_t> meta;
    std::vector<uint8_t> owned;
    std::vector<Piece> pieces;
    std::shared_ptr<TransferState> state;  // failed if the send itself fails
  };
  struct Connection {
    int fd = -1;
    PeerId peer;  // empty until HELLO on inbound connections
    bool inbound = false;
    std::atomic<bool> closed{false};
    std::mutex qmu;
    std::condition_variable qcv;
    std::deque<OutFrame> queue;
    std::thread reader;
    std::thread sender;
    // Outbound only: fulfilled when the peer's HELLO reply arrives (or the
    // connection dies first), so connect() can wait for a usable link.
    std::promise<Status> handshake;
    std::atomic<bool> handshake_done{false};
  };
  struct Pending {
    std::shared_ptr<TransferState> state;
    std::weak_ptr<Connection> conn;
  };

  void accept_loop();
  void reader_loop(std::shared_ptr<Connection> conn);
  void sender_loop(std::shared_ptr<Connection> conn);
  void reaper_loop();

  void start_connection(const std::shared_ptr<Connection>& conn);
  void finish_handshake(const std::shared_ptr<Connection>& conn, Status status);
  void close_connection(const std::shared_ptr<Connection>& conn, const Status& why);
  void reap(const std::shared_ptr<Connection>& conn);
  void enqueue(const std::shared_ptr<Connection>& conn, OutFrame frame);
  std::shared_ptr<Connection> find_connection(const PeerId& peer) const;

  bool handle_frame(const std::shared_ptr<Connection>& conn, const wire::FrameHeader& header);
  bool handle_hello(const std::shared_ptr<Connection>& conn, const wire::FrameHeader& header);
  bool handle_write(const std::shared_ptr<Connection>& conn, const wire::FrameHeader& header);
  bool handle_write_ack(const std::shared_ptr<Connection>& conn, const wire::FrameHeader& header);
  bool handle_read_req(const std::shared_ptr<Connection>& conn, const wire::FrameHeader& header);
  bool handle_read_resp(const std::shared_ptr<Connection>& conn, const wire::FrameHeader& header);
  bool handle_notify(const std::shared_ptr<Connection>& conn, const wire::FrameHeader& header);

  OutFrame make_hello();
  std::shared_ptr<TransferState> take_pending(TransferId id);
  void add_pending(const std::shared_ptr<TransferState>& state,
                   const std::shared_ptr<Connection>& conn);
  void fail_pending_for(const std::shared_ptr<Connection>& conn, const Status& why);
  void expire_pending();
  std::optional<TransferState::Clock::time_point> deadline_for(const TransferOptions& opts) const;

  TcpTransportConfig config_;
  int listen_fd_ = -1;
  Endpoint local_endpoint_;
  std::atomic<bool> running_{false};
  std::thread acceptor_;
  std::thread reaper_;

  mutable std::mutex mu_;  // guards connections_, all_connections_, dead_
  std::unordered_map<PeerId, std::shared_ptr<Connection>> connections_;
  std::vector<std::shared_ptr<Connection>> all_connections_;
  std::vector<std::shared_ptr<Connection>> dead_;

  mutable std::mutex pending_mu_;
  std::unordered_map<TransferId, Pending> pending_;
  std::atomic<TransferId> next_id_{1};

  MemoryRegistry regions_;

  mutable std::shared_mutex handler_mu_;  // shared while a handler runs
  NotifyHandler notify_handler_;
  DisconnectHandler disconnect_handler_;
  std::shared_ptr<int> alive_ = std::make_shared<int>(0);  // liveness token for late hooks
};

}  // namespace kvc
