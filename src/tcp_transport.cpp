#include "kvc/tcp_transport.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>

#include "kvc/logging.hpp"

namespace kvc {

namespace {

#ifdef MSG_NOSIGNAL
constexpr int kSendFlags = MSG_NOSIGNAL;
#else
constexpr int kSendFlags = 0;
#endif

std::string errno_string(int err) {
  return std::string(std::strerror(err)) + " (errno " + std::to_string(err) + ")";
}

bool read_exact(int fd, void* buf, uint64_t len) {
  auto* p = static_cast<uint8_t*>(buf);
  while (len > 0) {
    const ssize_t n = ::recv(fd, p, static_cast<size_t>(len), 0);
    if (n < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (n == 0) return false;
    p += n;
    len -= static_cast<uint64_t>(n);
  }
  return true;
}

bool discard_exact(int fd, uint64_t len) {
  uint8_t scratch[64 * 1024];
  while (len > 0) {
    const size_t want = static_cast<size_t>(std::min<uint64_t>(len, sizeof scratch));
    const ssize_t n = ::recv(fd, scratch, want, 0);
    if (n < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (n == 0) return false;
    len -= static_cast<uint64_t>(n);
  }
  return true;
}

// Writes every iovec fully, handling partial sends and the IOV_MAX limit.
bool send_all(int fd, std::vector<iovec>& iov) {
  size_t idx = 0;
  while (idx < iov.size()) {
    if (iov[idx].iov_len == 0) {
      ++idx;
      continue;
    }
    msghdr msg{};
    msg.msg_iov = &iov[idx];
    msg.msg_iovlen =
        static_cast<decltype(msg.msg_iovlen)>(std::min<size_t>(iov.size() - idx, IOV_MAX));
    const ssize_t w = ::sendmsg(fd, &msg, kSendFlags);
    if (w < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (w == 0) return false;
    size_t rem = static_cast<size_t>(w);
    while (rem > 0 && idx < iov.size()) {
      if (rem >= iov[idx].iov_len) {
        rem -= iov[idx].iov_len;
        ++idx;
      } else {
        iov[idx].iov_base = static_cast<char*>(iov[idx].iov_base) + rem;
        iov[idx].iov_len -= rem;
        rem = 0;
      }
    }
  }
  return true;
}

void configure_socket(int fd, const TcpTransportConfig& cfg) {
  int one = 1;
  if (cfg.tcp_nodelay) ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
#ifdef SO_NOSIGPIPE
  ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof one);
#endif
  if (cfg.socket_buffer_bytes > 0) {
    ::setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &cfg.socket_buffer_bytes, sizeof cfg.socket_buffer_bytes);
    ::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &cfg.socket_buffer_bytes, sizeof cfg.socket_buffer_bytes);
  }
}

bool set_nonblocking(int fd, bool enable) {
  const int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0) return false;
  const int updated = enable ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK);
  return ::fcntl(fd, F_SETFL, updated) == 0;
}

uint16_t port_of(const sockaddr_storage& ss) {
  if (ss.ss_family == AF_INET) {
    return ntohs(reinterpret_cast<const sockaddr_in*>(&ss)->sin_port);
  }
  return ntohs(reinterpret_cast<const sockaddr_in6*>(&ss)->sin6_port);
}

Status status_from_wire(uint32_t code, std::string message) {
  if (code == 0) return Status::Ok();
  if (code > static_cast<uint32_t>(StatusCode::kInternal)) {
    return Status::Internal("unknown remote status code " + std::to_string(code) + ": " + message);
  }
  return Status(static_cast<StatusCode>(code), std::move(message));
}

}  // namespace

TcpTransport::TcpTransport(TcpTransportConfig config) : config_(std::move(config)) {}

TcpTransport::~TcpTransport() { stop(); }

Status TcpTransport::start() {
  if (config_.self.empty()) return Status::InvalidArgument("self peer id is empty");
  if (running_.exchange(true)) return Status::Ok();

  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE | AI_NUMERICSERV;
  addrinfo* res = nullptr;
  const std::string port = std::to_string(config_.bind_port);
  const int rc = ::getaddrinfo(config_.bind_host.empty() ? nullptr : config_.bind_host.c_str(),
                               port.c_str(), &hints, &res);
  if (rc != 0) {
    running_ = false;
    return Status::Unavailable("cannot resolve bind host '" + config_.bind_host + "': " +
                               gai_strerror(rc));
  }
  int fd = -1;
  Status err;
  for (addrinfo* ai = res; ai != nullptr; ai = ai->ai_next) {
    fd = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd < 0) {
      err = Status::Unavailable("socket: " + errno_string(errno));
      continue;
    }
    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    if (::bind(fd, ai->ai_addr, ai->ai_addrlen) == 0 && ::listen(fd, 128) == 0) break;
    err = Status::Unavailable("bind/listen on " + config_.bind_host + ":" + port + ": " +
                              errno_string(errno));
    ::close(fd);
    fd = -1;
  }
  ::freeaddrinfo(res);
  if (fd < 0) {
    running_ = false;
    return err.ok() ? Status::Unavailable("no usable bind address") : err;
  }

  sockaddr_storage ss{};
  socklen_t sl = sizeof ss;
  ::getsockname(fd, reinterpret_cast<sockaddr*>(&ss), &sl);
  local_endpoint_ = Endpoint{config_.bind_host, port_of(ss)};
  listen_fd_ = fd;

  acceptor_ = std::thread([this] { accept_loop(); });
  reaper_ = std::thread([this] { reaper_loop(); });
  KVC_LOG_INFO << "tcp transport '" << config_.self << "' listening on "
               << local_endpoint_.to_string();
  return Status::Ok();
}

void TcpTransport::stop() {
  if (!running_.exchange(false)) return;
  if (acceptor_.joinable()) acceptor_.join();
  if (listen_fd_ >= 0) {
    ::close(listen_fd_);
    listen_fd_ = -1;
  }
  std::vector<std::shared_ptr<Connection>> conns;
  {
    std::lock_guard<std::mutex> lock(mu_);
    conns = all_connections_;
  }
  const Status why = Status::Unavailable("transport stopped");
  for (auto& c : conns) close_connection(c, why);
  if (reaper_.joinable()) reaper_.join();
  for (auto& c : conns) reap(c);
  {
    std::lock_guard<std::mutex> lock(mu_);
    connections_.clear();
    all_connections_.clear();
    dead_.clear();
  }
  std::vector<std::shared_ptr<TransferState>> leftovers;
  {
    std::lock_guard<std::mutex> lock(pending_mu_);
    for (auto& [_, p] : pending_) leftovers.push_back(p.state);
    pending_.clear();
  }
  for (auto& s : leftovers) s->complete(why, 0);
  KVC_LOG_INFO << "tcp transport '" << config_.self << "' stopped";
}

Endpoint TcpTransport::local_endpoint() const { return local_endpoint_; }

Status TcpTransport::connect(const PeerId& peer, const Endpoint& endpoint) {
  if (!running_) return Status::Unavailable("transport not started");
  if (peer.empty()) return Status::InvalidArgument("peer id is empty");
  if (peer == config_.self) return Status::InvalidArgument("cannot connect to self");

  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_NUMERICSERV;
  addrinfo* res = nullptr;
  const std::string port = std::to_string(endpoint.port);
  const int rc = ::getaddrinfo(endpoint.host.c_str(), port.c_str(), &hints, &res);
  if (rc != 0) {
    return Status::Unavailable("cannot resolve '" + endpoint.host + "': " + gai_strerror(rc));
  }
  int fd = -1;
  Status err = Status::Unavailable("no addresses for " + endpoint.to_string());
  for (addrinfo* ai = res; ai != nullptr; ai = ai->ai_next) {
    fd = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd < 0) {
      err = Status::Unavailable("socket: " + errno_string(errno));
      continue;
    }
    set_nonblocking(fd, true);
    int crc = ::connect(fd, ai->ai_addr, ai->ai_addrlen);
    if (crc < 0 && errno != EINPROGRESS) {
      err = Status::Unavailable("connect to " + endpoint.to_string() + ": " + errno_string(errno));
      ::close(fd);
      fd = -1;
      continue;
    }
    if (crc < 0) {
      pollfd pfd{fd, POLLOUT, 0};
      const int prc = ::poll(&pfd, 1, static_cast<int>(config_.connect_timeout.count()));
      if (prc <= 0) {
        err = prc == 0 ? Status::Timeout("connect to " + endpoint.to_string() + " timed out")
                       : Status::Unavailable("poll: " + errno_string(errno));
        ::close(fd);
        fd = -1;
        continue;
      }
      int soerr = 0;
      socklen_t len = sizeof soerr;
      ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &soerr, &len);
      if (soerr != 0) {
        err = Status::Unavailable("connect to " + endpoint.to_string() + ": " + errno_string(soerr));
        ::close(fd);
        fd = -1;
        continue;
      }
    }
    set_nonblocking(fd, false);
    break;
  }
  ::freeaddrinfo(res);
  if (fd < 0) return err;

  configure_socket(fd, config_);
  auto conn = std::make_shared<Connection>();
  conn->fd = fd;
  conn->peer = peer;
  conn->inbound = false;
  auto handshake = conn->handshake.get_future();
  {
    std::lock_guard<std::mutex> lock(mu_);
    connections_[peer] = conn;
    all_connections_.push_back(conn);
  }
  start_connection(conn);

  // Wait until the peer has registered us and replied with its HELLO, so a
  // transfer issued by either side right after connect() finds the link.
  if (handshake.wait_for(config_.connect_timeout) != std::future_status::ready) {
    close_connection(conn, Status::Timeout("no HELLO from '" + peer + "' within " +
                                           std::to_string(config_.connect_timeout.count()) + "ms"));
    return Status::Timeout("handshake with '" + peer + "' at " + endpoint.to_string() +
                           " timed out");
  }
  const Status hs = handshake.get();
  if (!hs.ok()) return hs;
  KVC_LOG_INFO << "'" << config_.self << "' connected to '" << peer << "' at "
               << endpoint.to_string();
  return Status::Ok();
}

Status TcpTransport::disconnect(const PeerId& peer) {
  std::shared_ptr<Connection> conn;
  {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = connections_.find(peer);
    if (it == connections_.end()) return Status::NotFound("not connected to '" + peer + "'");
    conn = it->second;
  }
  close_connection(conn, Status::Disconnected("disconnected locally"));
  return Status::Ok();
}

bool TcpTransport::is_connected(const PeerId& peer) const { return find_connection(peer) != nullptr; }

Status TcpTransport::register_memory(const MemoryRegion& region) {
  if (region.kind == MemoryKind::kDevice) {
    return Status::Unimplemented("tcp transport can only address host memory");
  }
  return regions_.add(region);
}

Status TcpTransport::deregister_memory(RegionId id) { return regions_.remove(id); }

std::optional<TransferState::Clock::time_point> TcpTransport::deadline_for(
    const TransferOptions& opts) const {
  if (opts.timeout.count() <= 0) return std::nullopt;
  return TransferState::Clock::now() + opts.timeout;
}

TransferHandle TcpTransport::write(const PeerId& dst, std::vector<XferDesc> descs,
                                   const TransferOptions& opts) {
  if (descs.empty()) return TransferHandle::failed(Status::InvalidArgument("no descriptors"), dst);
  auto conn = find_connection(dst);
  if (!conn) return TransferHandle::failed(Status::NotFound("not connected to peer '" + dst + "'"), dst);

  std::vector<const std::byte*> ptrs;
  ptrs.reserve(descs.size());
  uint64_t total = 0;
  for (const auto& d : descs) {
    if (d.length == 0) return TransferHandle::failed(Status::InvalidArgument("zero-length descriptor"), dst);
    auto reg = regions_.get(d.src_region);
    if (!reg) {
      return TransferHandle::failed(
          Status::NotFound("local region " + std::to_string(d.src_region) + " not registered"), dst);
    }
    if (!reg->contains(d.src_offset, d.length)) {
      return TransferHandle::failed(
          Status::OutOfRange("source range exceeds local region " + std::to_string(d.src_region)),
          dst);
    }
    ptrs.push_back(reg->at(d.src_offset));
    total += d.length;
  }

  const TransferId id = next_id_.fetch_add(1);
  auto state = std::make_shared<TransferState>(id, dst, total, deadline_for(opts));

  OutFrame frame;
  frame.state = state;
  const uint64_t payload = 4 + descs.size() * wire::kChunkHeaderSize + total;
  frame.meta.resize(wire::kFrameHeaderSize + 4);
  wire::encode_frame_header({wire::FrameType::kWrite, id, payload}, frame.meta.data());
  const uint32_t n = static_cast<uint32_t>(descs.size());
  std::memcpy(frame.meta.data() + wire::kFrameHeaderSize, &n, sizeof n);
  frame.owned.resize(descs.size() * wire::kChunkHeaderSize);
  frame.pieces.reserve(1 + 2 * descs.size());
  frame.pieces.push_back({frame.meta.data(), frame.meta.size()});
  for (size_t i = 0; i < descs.size(); ++i) {
    uint8_t* hdr = frame.owned.data() + i * wire::kChunkHeaderSize;
    wire::encode_chunk_header({descs[i].dst_region, descs[i].dst_offset, descs[i].length}, hdr);
    frame.pieces.push_back({hdr, wire::kChunkHeaderSize});
    frame.pieces.push_back({ptrs[i], static_cast<size_t>(descs[i].length)});
  }

  add_pending(state, conn);
  state->set_cancel_hook([this, id, token = std::weak_ptr<int>(alive_)] {
    if (token.lock()) take_pending(id);
  });
  enqueue(conn, std::move(frame));
  return TransferHandle(state);
}

TransferHandle TcpTransport::read(const PeerId& src, std::vector<XferDesc> descs,
                                  const TransferOptions& opts) {
  if (descs.empty()) return TransferHandle::failed(Status::InvalidArgument("no descriptors"), src);
  auto conn = find_connection(src);
  if (!conn) return TransferHandle::failed(Status::NotFound("not connected to peer '" + src + "'"), src);

  uint64_t total = 0;
  for (const auto& d : descs) {
    if (d.length == 0) return TransferHandle::failed(Status::InvalidArgument("zero-length descriptor"), src);
    auto reg = regions_.get(d.dst_region);
    if (!reg) {
      return TransferHandle::failed(
          Status::NotFound("local region " + std::to_string(d.dst_region) + " not registered"), src);
    }
    if (!reg->contains(d.dst_offset, d.length)) {
      return TransferHandle::failed(
          Status::OutOfRange("destination range exceeds local region " +
                             std::to_string(d.dst_region)),
          src);
    }
    total += d.length;
  }

  const TransferId id = next_id_.fetch_add(1);
  auto state = std::make_shared<TransferState>(id, src, total, deadline_for(opts));

  OutFrame frame;
  frame.state = state;
  const uint64_t payload = 4 + descs.size() * wire::kReadDescSize;
  frame.meta.resize(wire::kFrameHeaderSize + payload);
  wire::encode_frame_header({wire::FrameType::kReadReq, id, payload}, frame.meta.data());
  uint8_t* p = frame.meta.data() + wire::kFrameHeaderSize;
  const uint32_t n = static_cast<uint32_t>(descs.size());
  std::memcpy(p, &n, sizeof n);
  p += 4;
  for (const auto& d : descs) {
    wire::encode_read_desc(d, p);
    p += wire::kReadDescSize;
  }
  frame.pieces.push_back({frame.meta.data(), frame.meta.size()});

  add_pending(state, conn);
  state->set_cancel_hook([this, id, token = std::weak_ptr<int>(alive_)] {
    if (token.lock()) take_pending(id);
  });
  enqueue(conn, std::move(frame));
  return TransferHandle(state);
}

Status TcpTransport::notify(const PeerId& dst, std::string_view payload) {
  auto conn = find_connection(dst);
  if (!conn) return Status::NotFound("not connected to peer '" + dst + "'");
  if (payload.size() > wire::kMaxControlPayload) {
    return Status::InvalidArgument("notify payload exceeds " +
                                   std::to_string(wire::kMaxControlPayload) + " bytes");
  }
  OutFrame frame;
  frame.meta.resize(wire::kFrameHeaderSize + payload.size());
  wire::encode_frame_header({wire::FrameType::kNotify, 0, payload.size()}, frame.meta.data());
  std::memcpy(frame.meta.data() + wire::kFrameHeaderSize, payload.data(), payload.size());
  frame.pieces.push_back({frame.meta.data(), frame.meta.size()});
  enqueue(conn, std::move(frame));
  return Status::Ok();
}

void TcpTransport::set_notify_handler(NotifyHandler handler) {
  std::unique_lock<std::shared_mutex> lock(handler_mu_);
  notify_handler_ = std::move(handler);
}

void TcpTransport::set_disconnect_handler(DisconnectHandler handler) {
  std::unique_lock<std::shared_mutex> lock(handler_mu_);
  disconnect_handler_ = std::move(handler);
}

size_t TcpTransport::pending_transfers() const {
  std::lock_guard<std::mutex> lock(pending_mu_);
  return pending_.size();
}

// ---------------------------------------------------------------------------
// Connection lifecycle
// ---------------------------------------------------------------------------

TcpTransport::OutFrame TcpTransport::make_hello() {
  wire::Writer w;
  w.u32(0).str(config_.self);
  OutFrame frame;
  frame.meta.resize(wire::kFrameHeaderSize + w.size());
  wire::encode_frame_header({wire::FrameType::kHello, 0, w.size()}, frame.meta.data());
  std::memcpy(frame.meta.data() + wire::kFrameHeaderSize, w.data().data(), w.size());
  frame.pieces.push_back({frame.meta.data(), frame.meta.size()});
  return frame;
}

void TcpTransport::start_connection(const std::shared_ptr<Connection>& conn) {
  {
    // Hold qmu while the thread objects are assigned; reader_loop takes qmu
    // once before doing anything so reap() can never race the assignment.
    std::lock_guard<std::mutex> lock(conn->qmu);
    conn->sender = std::thread([this, conn] { sender_loop(conn); });
    conn->reader = std::thread([this, conn] { reader_loop(conn); });
  }
  // The connecting side speaks first; the accepting side answers from
  // handle_hello() once it has registered the peer.
  if (!conn->inbound) enqueue(conn, make_hello());
}

void TcpTransport::finish_handshake(const std::shared_ptr<Connection>& conn, Status status) {
  if (conn->inbound || conn->handshake_done.exchange(true)) return;
  conn->handshake.set_value(std::move(status));
}

void TcpTransport::enqueue(const std::shared_ptr<Connection>& conn, OutFrame frame) {
  {
    std::lock_guard<std::mutex> lock(conn->qmu);
    if (!conn->closed.load()) {
      conn->queue.push_back(std::move(frame));
      conn->qcv.notify_one();
      return;
    }
  }
  if (frame.state) {
    take_pending(frame.state->id());
    frame.state->complete(
        Status::Disconnected("connection to '" + frame.state->peer() + "' is closed"), 0);
  }
}

std::shared_ptr<TcpTransport::Connection> TcpTransport::find_connection(const PeerId& peer) const {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = connections_.find(peer);
  if (it == connections_.end() || it->second->closed.load()) return nullptr;
  return it->second;
}

void TcpTransport::close_connection(const std::shared_ptr<Connection>& conn, const Status& why) {
  {
    std::lock_guard<std::mutex> lock(conn->qmu);
    if (conn->closed.exchange(true)) return;
  }
  conn->qcv.notify_all();
  ::shutdown(conn->fd, SHUT_RDWR);

  PeerId peer;
  bool was_mapped = false;
  {
    std::lock_guard<std::mutex> lock(mu_);
    peer = conn->peer;
    auto it = connections_.find(peer);
    if (it != connections_.end() && it->second == conn) {
      connections_.erase(it);
      was_mapped = true;
    }
  }
  fail_pending_for(conn, why);
  finish_handshake(conn, why);
  if (!peer.empty()) {
    KVC_LOG_INFO << "'" << config_.self << "' connection to '" << peer << "' closed: "
                 << why.to_string();
  }
  if (was_mapped && running_.load()) {
    std::shared_lock<std::shared_mutex> lock(handler_mu_);
    if (disconnect_handler_) disconnect_handler_(peer);
  }
}

void TcpTransport::reap(const std::shared_ptr<Connection>& conn) {
  if (conn->sender.joinable()) conn->sender.join();
  if (conn->reader.joinable()) conn->reader.join();
  if (conn->fd >= 0) {
    ::close(conn->fd);
    conn->fd = -1;
  }
}

void TcpTransport::accept_loop() {
  while (running_.load()) {
    pollfd pfd{listen_fd_, POLLIN, 0};
    const int prc = ::poll(&pfd, 1, 100);
    if (prc <= 0) continue;
    sockaddr_storage ss{};
    socklen_t sl = sizeof ss;
    const int fd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&ss), &sl);
    if (fd < 0) {
      if (errno == EINTR || errno == EAGAIN || errno == ECONNABORTED) continue;
      if (!running_.load()) break;
      KVC_LOG_WARN << "accept failed: " << errno_string(errno);
      continue;
    }
    configure_socket(fd, config_);
    auto conn = std::make_shared<Connection>();
    conn->fd = fd;
    conn->inbound = true;
    {
      std::lock_guard<std::mutex> lock(mu_);
      all_connections_.push_back(conn);
    }
    start_connection(conn);
  }
}

void TcpTransport::sender_loop(std::shared_ptr<Connection> conn) {
  while (true) {
    OutFrame frame;
    {
      std::unique_lock<std::mutex> lock(conn->qmu);
      conn->qcv.wait(lock, [&] { return conn->closed.load() || !conn->queue.empty(); });
      if (conn->closed.load()) break;
      frame = std::move(conn->queue.front());
      conn->queue.pop_front();
    }
    std::vector<iovec> iov;
    iov.reserve(frame.pieces.size());
    for (const auto& p : frame.pieces) iov.push_back({const_cast<void*>(p.data), p.len});
    if (!send_all(conn->fd, iov)) {
      const int err = errno;
      close_connection(conn, Status::Disconnected("send failed: " + errno_string(err)));
      break;
    }
  }
  std::deque<OutFrame> rest;
  {
    std::lock_guard<std::mutex> lock(conn->qmu);
    rest.swap(conn->queue);
  }
  for (auto& f : rest) {
    if (f.state) {
      take_pending(f.state->id());
      f.state->complete(Status::Disconnected("connection closed before send"), 0);
    }
  }
}

void TcpTransport::reader_loop(std::shared_ptr<Connection> conn) {
  { std::lock_guard<std::mutex> lock(conn->qmu); }  // see start_connection
  uint8_t hdr[wire::kFrameHeaderSize];
  while (!conn->closed.load()) {
    if (!read_exact(conn->fd, hdr, sizeof hdr)) break;
    auto header = wire::decode_frame_header(hdr);
    if (!header.ok()) {
      KVC_LOG_ERROR << "'" << config_.self << "' bad frame from '" << conn->peer
                    << "': " << header.status().to_string();
      break;
    }
    if (!handle_frame(conn, *header)) break;
  }
  close_connection(conn, Status::Disconnected("peer '" + conn->peer + "' closed the connection"));
  std::lock_guard<std::mutex> lock(mu_);
  dead_.push_back(conn);
}

void TcpTransport::reaper_loop() {
  while (running_.load()) {
    std::this_thread::sleep_for(config_.reaper_interval);
    std::vector<std::shared_ptr<Connection>> dead;
    {
      std::lock_guard<std::mutex> lock(mu_);
      dead.swap(dead_);
    }
    for (auto& c : dead) {
      reap(c);
      std::lock_guard<std::mutex> lock(mu_);
      all_connections_.erase(std::remove(all_connections_.begin(), all_connections_.end(), c),
                             all_connections_.end());
    }
    expire_pending();
  }
}

// ---------------------------------------------------------------------------
// Pending-transfer bookkeeping
// ---------------------------------------------------------------------------

void TcpTransport::add_pending(const std::shared_ptr<TransferState>& state,
                               const std::shared_ptr<Connection>& conn) {
  std::lock_guard<std::mutex> lock(pending_mu_);
  pending_[state->id()] = Pending{state, conn};
}

std::shared_ptr<TransferState> TcpTransport::take_pending(TransferId id) {
  std::lock_guard<std::mutex> lock(pending_mu_);
  auto it = pending_.find(id);
  if (it == pending_.end()) return nullptr;
  auto state = std::move(it->second.state);
  pending_.erase(it);
  return state;
}

void TcpTransport::fail_pending_for(const std::shared_ptr<Connection>& conn, const Status& why) {
  std::vector<std::shared_ptr<TransferState>> victims;
  {
    std::lock_guard<std::mutex> lock(pending_mu_);
    for (auto it = pending_.begin(); it != pending_.end();) {
      auto c = it->second.conn.lock();
      if (!c || c == conn) {
        victims.push_back(std::move(it->second.state));
        it = pending_.erase(it);
      } else {
        ++it;
      }
    }
  }
  for (auto& v : victims) v->complete(why, 0);
}

void TcpTransport::expire_pending() {
  const auto now = TransferState::Clock::now();
  std::vector<std::shared_ptr<TransferState>> expired;
  {
    std::lock_guard<std::mutex> lock(pending_mu_);
    for (auto it = pending_.begin(); it != pending_.end();) {
      const auto& dl = it->second.state->deadline();
      if (dl && *dl <= now) {
        expired.push_back(std::move(it->second.state));
        it = pending_.erase(it);
      } else {
        ++it;
      }
    }
  }
  for (auto& s : expired) {
    s->complete(Status::Timeout("transfer " + std::to_string(s->id()) + " to '" + s->peer() +
                                "' exceeded its deadline"),
                0);
  }
}

// ---------------------------------------------------------------------------
// Frame handlers (run on the connection's reader thread). Returning false
// closes the connection because the stream can no longer be trusted.
// ---------------------------------------------------------------------------

bool TcpTransport::handle_frame(const std::shared_ptr<Connection>& conn,
                                const wire::FrameHeader& h) {
  switch (h.type) {
    case wire::FrameType::kHello: return handle_hello(conn, h);
    case wire::FrameType::kWrite: return handle_write(conn, h);
    case wire::FrameType::kWriteAck: return handle_write_ack(conn, h);
    case wire::FrameType::kReadReq: return handle_read_req(conn, h);
    case wire::FrameType::kReadResp: return handle_read_resp(conn, h);
    case wire::FrameType::kNotify: return handle_notify(conn, h);
  }
  return false;
}

bool TcpTransport::handle_hello(const std::shared_ptr<Connection>& conn,
                                const wire::FrameHeader& h) {
  if (h.payload_length > wire::kMaxControlPayload) return false;
  std::vector<uint8_t> buf(static_cast<size_t>(h.payload_length));
  if (!read_exact(conn->fd, buf.data(), buf.size())) return false;
  wire::Reader r(buf);
  uint32_t flags = 0;
  std::string peer;
  if (!r.u32(flags) || !r.str(peer, 1024) || peer.empty()) {
    KVC_LOG_ERROR << "malformed HELLO";
    return false;
  }
  if (conn->inbound) {
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (!conn->peer.empty()) {
        KVC_LOG_ERROR << "duplicate HELLO from '" << peer << "'";
        return false;
      }
      conn->peer = peer;
      connections_[peer] = conn;  // newest connection wins for outbound frames
    }
    KVC_LOG_INFO << "'" << config_.self << "' accepted connection from '" << peer << "'";
    enqueue(conn, make_hello());
    return true;
  }
  if (peer != conn->peer) {
    KVC_LOG_ERROR << "'" << config_.self << "' expected peer '" << conn->peer
                  << "' but remote identifies as '" << peer << "'";
    return false;
  }
  finish_handshake(conn, Status::Ok());
  return true;
}

bool TcpTransport::handle_write(const std::shared_ptr<Connection>& conn,
                                const wire::FrameHeader& h) {
  if (h.payload_length < 4) return false;
  uint32_t n = 0;
  if (!read_exact(conn->fd, &n, sizeof n)) return false;
  if (n > wire::kMaxChunks) return false;
  uint64_t remaining = h.payload_length - 4;
  Status status;
  uint64_t bytes = 0;
  uint8_t chunk_hdr[wire::kChunkHeaderSize];
  for (uint32_t i = 0; i < n; ++i) {
    if (remaining < wire::kChunkHeaderSize) return false;
    if (!read_exact(conn->fd, chunk_hdr, sizeof chunk_hdr)) return false;
    remaining -= wire::kChunkHeaderSize;
    const wire::ChunkHeader ch = wire::decode_chunk_header(chunk_hdr);
    if (ch.length > remaining) return false;
    auto reg = regions_.get(ch.region);
    if (reg && reg->contains(ch.offset, ch.length)) {
      if (!read_exact(conn->fd, reg->at(ch.offset), ch.length)) return false;
      bytes += ch.length;
    } else {
      if (status.ok()) {
        status = reg ? Status::OutOfRange("chunk [" + std::to_string(ch.offset) + ", +" +
                                          std::to_string(ch.length) + ") exceeds region " +
                                          std::to_string(ch.region) + " of " +
                                          std::to_string(reg->length) + " bytes on '" +
                                          config_.self + "'")
                     : Status::NotFound("region " + std::to_string(ch.region) +
                                        " not registered on '" + config_.self + "'");
      }
      if (!discard_exact(conn->fd, ch.length)) return false;
    }
    remaining -= ch.length;
  }
  if (remaining != 0) return false;

  wire::Writer w;
  w.u32(static_cast<uint32_t>(status.code())).u64(bytes).str(status.message());
  OutFrame ack;
  ack.meta.resize(wire::kFrameHeaderSize + w.size());
  wire::encode_frame_header({wire::FrameType::kWriteAck, h.transfer_id, w.size()}, ack.meta.data());
  std::memcpy(ack.meta.data() + wire::kFrameHeaderSize, w.data().data(), w.size());
  ack.pieces.push_back({ack.meta.data(), ack.meta.size()});
  enqueue(conn, std::move(ack));
  return true;
}

bool TcpTransport::handle_write_ack(const std::shared_ptr<Connection>& conn,
                                    const wire::FrameHeader& h) {
  if (h.payload_length > wire::kMaxControlPayload) return false;
  std::vector<uint8_t> buf(static_cast<size_t>(h.payload_length));
  if (!read_exact(conn->fd, buf.data(), buf.size())) return false;
  wire::Reader r(buf);
  uint32_t code = 0;
  uint64_t bytes = 0;
  std::string msg;
  if (!r.u32(code) || !r.u64(bytes) || !r.str(msg)) return false;
  if (auto state = take_pending(h.transfer_id)) {
    state->complete(status_from_wire(code, std::move(msg)), bytes);
  }
  return true;
}

bool TcpTransport::handle_read_req(const std::shared_ptr<Connection>& conn,
                                   const wire::FrameHeader& h) {
  if (h.payload_length < 4) return false;
  uint32_t n = 0;
  if (!read_exact(conn->fd, &n, sizeof n)) return false;
  if (n > wire::kMaxChunks || h.payload_length != 4 + uint64_t{n} * wire::kReadDescSize) return false;
  std::vector<uint8_t> buf(static_cast<size_t>(n) * wire::kReadDescSize);
  if (!read_exact(conn->fd, buf.data(), buf.size())) return false;

  Status status;
  std::vector<XferDesc> descs;
  std::vector<const std::byte*> ptrs;
  descs.reserve(n);
  ptrs.reserve(n);
  uint64_t total = 0;
  for (uint32_t i = 0; i < n; ++i) {
    const XferDesc d = wire::decode_read_desc(buf.data() + i * wire::kReadDescSize);
    auto reg = regions_.get(d.src_region);
    if (!reg) {
      status = Status::NotFound("region " + std::to_string(d.src_region) + " not registered on '" +
                                config_.self + "'");
      break;
    }
    if (!reg->contains(d.src_offset, d.length) || d.length == 0) {
      status = Status::OutOfRange("read range exceeds region " + std::to_string(d.src_region) +
                                  " on '" + config_.self + "'");
      break;
    }
    ptrs.push_back(reg->at(d.src_offset));
    descs.push_back(d);
    total += d.length;
  }

  OutFrame resp;
  wire::Writer prefix;
  if (!status.ok()) {
    prefix.u32(static_cast<uint32_t>(status.code())).u32(0);
    prefix.u32(static_cast<uint32_t>(status.message().size()));
    prefix.bytes(status.message().data(), status.message().size());
    resp.meta.resize(wire::kFrameHeaderSize + prefix.size());
    wire::encode_frame_header({wire::FrameType::kReadResp, h.transfer_id, prefix.size()},
                              resp.meta.data());
    std::memcpy(resp.meta.data() + wire::kFrameHeaderSize, prefix.data().data(), prefix.size());
    resp.pieces.push_back({resp.meta.data(), resp.meta.size()});
  } else {
    prefix.u32(0).u32(n).u32(0);
    const uint64_t payload = prefix.size() + uint64_t{n} * wire::kChunkHeaderSize + total;
    resp.meta.resize(wire::kFrameHeaderSize + prefix.size());
    wire::encode_frame_header({wire::FrameType::kReadResp, h.transfer_id, payload}, resp.meta.data());
    std::memcpy(resp.meta.data() + wire::kFrameHeaderSize, prefix.data().data(), prefix.size());
    resp.owned.resize(static_cast<size_t>(n) * wire::kChunkHeaderSize);
    resp.pieces.reserve(1 + 2 * n);
    resp.pieces.push_back({resp.meta.data(), resp.meta.size()});
    for (uint32_t i = 0; i < n; ++i) {
      uint8_t* hdr = resp.owned.data() + i * wire::kChunkHeaderSize;
      wire::encode_chunk_header({descs[i].dst_region, descs[i].dst_offset, descs[i].length}, hdr);
      resp.pieces.push_back({hdr, wire::kChunkHeaderSize});
      resp.pieces.push_back({ptrs[i], static_cast<size_t>(descs[i].length)});
    }
  }
  enqueue(conn, std::move(resp));
  return true;
}

bool TcpTransport::handle_read_resp(const std::shared_ptr<Connection>& conn,
                                    const wire::FrameHeader& h) {
  if (h.payload_length < 12) return false;
  uint8_t prefix[12];
  if (!read_exact(conn->fd, prefix, sizeof prefix)) return false;
  wire::Reader pr(prefix, sizeof prefix);
  uint32_t code = 0, n = 0, msg_len = 0;
  pr.u32(code);
  pr.u32(n);
  pr.u32(msg_len);
  if (n > wire::kMaxChunks || msg_len > wire::kMaxControlPayload) return false;
  uint64_t remaining = h.payload_length - 12;
  if (msg_len > remaining) return false;
  std::string msg(msg_len, '\0');
  if (msg_len > 0 && !read_exact(conn->fd, msg.data(), msg_len)) return false;
  remaining -= msg_len;

  const Status remote = status_from_wire(code, std::move(msg));
  Status local;
  uint64_t bytes = 0;
  uint8_t chunk_hdr[wire::kChunkHeaderSize];
  for (uint32_t i = 0; i < n; ++i) {
    if (remaining < wire::kChunkHeaderSize) return false;
    if (!read_exact(conn->fd, chunk_hdr, sizeof chunk_hdr)) return false;
    remaining -= wire::kChunkHeaderSize;
    const wire::ChunkHeader ch = wire::decode_chunk_header(chunk_hdr);
    if (ch.length > remaining) return false;
    auto reg = regions_.get(ch.region);
    if (reg && reg->contains(ch.offset, ch.length)) {
      if (!read_exact(conn->fd, reg->at(ch.offset), ch.length)) return false;
      bytes += ch.length;
    } else {
      if (local.ok()) {
        local = Status::OutOfRange("read response chunk does not fit local region " +
                                   std::to_string(ch.region));
      }
      if (!discard_exact(conn->fd, ch.length)) return false;
    }
    remaining -= ch.length;
  }
  if (remaining != 0) return false;
  if (auto state = take_pending(h.transfer_id)) {
    state->complete(!remote.ok() ? remote : local, bytes);
  }
  return true;
}

bool TcpTransport::handle_notify(const std::shared_ptr<Connection>& conn,
                                 const wire::FrameHeader& h) {
  if (h.payload_length > wire::kMaxControlPayload) return false;
  if (conn->peer.empty()) {
    KVC_LOG_ERROR << "NOTIFY received before HELLO";
    return false;
  }
  std::string payload(static_cast<size_t>(h.payload_length), '\0');
  if (!payload.empty() && !read_exact(conn->fd, payload.data(), payload.size())) return false;
  std::shared_lock<std::shared_mutex> lock(handler_mu_);
  if (notify_handler_) notify_handler_(conn->peer, payload);
  return true;
}

}  // namespace kvc
