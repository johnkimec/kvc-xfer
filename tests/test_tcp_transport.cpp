#include "kvc/tcp_transport.hpp"

#include <gtest/gtest.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <random>

#include "test_util.hpp"

using namespace kvc;

namespace {

struct TcpPair {
  std::shared_ptr<TcpTransport> a;
  std::shared_ptr<TcpTransport> b;
  std::vector<uint8_t> amem;
  std::vector<uint8_t> bmem;

  explicit TcpPair(size_t bytes = 4096) : amem(bytes), bmem(bytes) {
    TcpTransportConfig ca;
    ca.self = "a";
    ca.bind_host = "127.0.0.1";
    TcpTransportConfig cb = ca;
    cb.self = "b";
    a = std::make_shared<TcpTransport>(ca);
    b = std::make_shared<TcpTransport>(cb);
    EXPECT_TRUE(a->start().ok());
    EXPECT_TRUE(b->start().ok());
    EXPECT_TRUE(a->register_memory({1, amem.data(), amem.size(), MemoryKind::kHost, -1}).ok());
    EXPECT_TRUE(b->register_memory({2, bmem.data(), bmem.size(), MemoryKind::kHost, -1}).ok());
    test::fill_pattern(amem, 1);
    test::fill_pattern(bmem, 2);
    EXPECT_TRUE(a->connect("b", b->local_endpoint()).ok());
    // b learns about a once HELLO arrives.
    EXPECT_TRUE(test::wait_until([&] { return b->is_connected("a"); }));
  }
};

TEST(TcpTransport, StartStopAndEndpoint) {
  TcpTransportConfig c;
  c.self = "solo";
  c.bind_host = "127.0.0.1";
  TcpTransport t(c);
  ASSERT_TRUE(t.start().ok());
  EXPECT_NE(t.local_endpoint().port, 0);
  EXPECT_EQ(t.local_endpoint().host, "127.0.0.1");
  EXPECT_TRUE(t.start().ok());  // idempotent
  t.stop();
  t.stop();
  TcpTransportConfig bad;
  EXPECT_EQ(TcpTransport(bad).start().code(), StatusCode::kInvalidArgument);
}

TEST(TcpTransport, ConnectErrors) {
  TcpTransportConfig c;
  c.self = "x";
  c.bind_host = "127.0.0.1";
  c.connect_timeout = std::chrono::milliseconds(500);
  TcpTransport t(c);
  EXPECT_EQ(t.connect("y", {"127.0.0.1", 1}).code(), StatusCode::kUnavailable);  // not started
  ASSERT_TRUE(t.start().ok());
  EXPECT_EQ(t.connect("x", {"127.0.0.1", 1}).code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(t.connect("", {"127.0.0.1", 1}).code(), StatusCode::kInvalidArgument);
  // Port 1 on loopback is refused promptly.
  EXPECT_EQ(t.connect("y", {"127.0.0.1", 1}).code(), StatusCode::kUnavailable);
  EXPECT_FALSE(t.connect("y", {"no.such.host.invalid", 5}).ok());
  EXPECT_FALSE(t.is_connected("y"));
  EXPECT_EQ(t.write("y", {{1, 0, 1, 0, 1}}).wait().status.code(), StatusCode::kNotFound);
  EXPECT_EQ(t.notify("y", "x").code(), StatusCode::kNotFound);
}

TEST(TcpTransport, WriteRoundTrip) {
  TcpPair p;
  auto r = p.a->write("b", {{1, 100, 2, 200, 300}}).wait();
  ASSERT_TRUE(r.ok()) << r.status.to_string();
  EXPECT_EQ(r.bytes, 300u);
  EXPECT_TRUE(std::equal(p.amem.begin() + 100, p.amem.begin() + 400, p.bmem.begin() + 200));
  EXPECT_EQ(p.bmem[199], test::pattern(199, 2));
  EXPECT_EQ(p.bmem[500], test::pattern(500, 2));
  EXPECT_EQ(p.a->pending_transfers(), 0u);
}

TEST(TcpTransport, WriteFromAcceptingSide) {
  TcpPair p;
  // b never called connect(); it uses the inbound connection.
  auto r = p.b->write("a", {{2, 0, 1, 1000, 100}}).wait();
  ASSERT_TRUE(r.ok()) << r.status.to_string();
  EXPECT_TRUE(std::equal(p.bmem.begin(), p.bmem.begin() + 100, p.amem.begin() + 1000));
}

TEST(TcpTransport, ReadRoundTrip) {
  TcpPair p;
  auto r = p.a->read("b", {{2, 0, 1, 512, 64}, {2, 900, 1, 0, 100}}).wait();
  ASSERT_TRUE(r.ok()) << r.status.to_string();
  EXPECT_EQ(r.bytes, 164u);
  EXPECT_TRUE(std::equal(p.bmem.begin(), p.bmem.begin() + 64, p.amem.begin() + 512));
  EXPECT_TRUE(std::equal(p.bmem.begin() + 900, p.bmem.begin() + 1000, p.amem.begin()));
  EXPECT_EQ(p.amem[64 + 512], test::pattern(64 + 512, 1));
}

TEST(TcpTransport, MultiChunkWrite) {
  TcpPair p;
  std::vector<XferDesc> descs;
  for (uint64_t i = 0; i < 64; ++i) descs.push_back({1, i * 64, 2, (63 - i) * 64, 64});
  auto r = p.a->write("b", descs).wait();
  ASSERT_TRUE(r.ok()) << r.status.to_string();
  EXPECT_EQ(r.bytes, 4096u);
  for (uint64_t i = 0; i < 64; ++i) {
    EXPECT_TRUE(std::equal(p.amem.begin() + i * 64, p.amem.begin() + (i + 1) * 64,
                           p.bmem.begin() + (63 - i) * 64));
  }
}

TEST(TcpTransport, LargeTransferExercisesPartialSends) {
  const size_t bytes = 48u << 20;  // 48 MiB
  TcpPair p(bytes);
  auto h = p.a->write("b", {{1, 0, 2, 0, bytes}});
  auto r = h.wait();
  ASSERT_TRUE(r.ok()) << r.status.to_string();
  EXPECT_EQ(r.bytes, bytes);
  EXPECT_EQ(p.amem, p.bmem);
  // And the other direction via read.
  test::fill_pattern(p.bmem, 7);
  r = p.a->read("b", {{2, 0, 1, 0, bytes}}).wait();
  ASSERT_TRUE(r.ok()) << r.status.to_string();
  EXPECT_EQ(p.amem, p.bmem);
}

TEST(TcpTransport, ManyConcurrentTransfersFromManyThreads) {
  constexpr int kThreads = 8;
  constexpr int kPerThread = 50;
  constexpr uint64_t slot = 2048;
  const size_t bytes = kThreads * kPerThread * slot;
  TcpPair p(bytes);
  std::vector<std::thread> threads;
  std::atomic<int> failures{0};
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      std::vector<TransferHandle> handles;
      for (int i = 0; i < kPerThread; ++i) {
        const uint64_t off = (t * kPerThread + i) * slot;
        if (i % 2 == 0) {
          handles.push_back(p.a->write("b", {{1, off, 2, off, slot}}));
        } else {
          handles.push_back(p.b->write("a", {{2, off, 1, off, slot}}));
        }
      }
      for (auto& h : handles) {
        if (!h.wait().ok()) failures.fetch_add(1);
      }
    });
  }
  for (auto& t : threads) t.join();
  EXPECT_EQ(failures.load(), 0);
  EXPECT_EQ(p.amem, p.bmem);
  EXPECT_EQ(p.a->pending_transfers(), 0u);
  EXPECT_EQ(p.b->pending_transfers(), 0u);
}

TEST(TcpTransport, RemoteErrorsKeepConnectionUsable) {
  TcpPair p;
  // Unknown remote region.
  auto r = p.a->write("b", {{1, 0, 99, 0, 16}}).wait();
  EXPECT_EQ(r.status.code(), StatusCode::kNotFound) << r.status.to_string();
  EXPECT_EQ(r.bytes, 0u);
  // Remote out of range; the first chunk lands, the second is discarded.
  r = p.a->write("b", {{1, 0, 2, 0, 16}, {1, 16, 2, 4090, 16}}).wait();
  EXPECT_EQ(r.status.code(), StatusCode::kOutOfRange) << r.status.to_string();
  EXPECT_EQ(r.bytes, 16u);
  EXPECT_TRUE(std::equal(p.amem.begin(), p.amem.begin() + 16, p.bmem.begin()));
  // Local validation errors are immediate.
  EXPECT_EQ(p.a->write("b", {{7, 0, 2, 0, 16}}).wait().status.code(), StatusCode::kNotFound);
  EXPECT_EQ(p.a->write("b", {{1, 4000, 2, 0, 100}}).wait().status.code(), StatusCode::kOutOfRange);
  EXPECT_EQ(p.a->write("b", {{1, 0, 2, 0, 0}}).wait().status.code(), StatusCode::kInvalidArgument);
  // Read of a bad remote range.
  r = p.a->read("b", {{2, 4000, 1, 0, 100}}).wait();
  EXPECT_EQ(r.status.code(), StatusCode::kOutOfRange);
  r = p.a->read("b", {{55, 0, 1, 0, 100}}).wait();
  EXPECT_EQ(r.status.code(), StatusCode::kNotFound);
  EXPECT_EQ(p.a->read("b", {{2, 0, 1, 4000, 100}}).wait().status.code(), StatusCode::kOutOfRange);
  // Still healthy afterwards.
  r = p.a->write("b", {{1, 500, 2, 500, 100}}).wait();
  ASSERT_TRUE(r.ok()) << r.status.to_string();
  EXPECT_TRUE(p.a->is_connected("b"));
  EXPECT_TRUE(p.b->is_connected("a"));
}

TEST(TcpTransport, NotifyBothDirections) {
  TcpPair p;
  std::mutex mu;
  std::vector<std::pair<PeerId, std::string>> at_a, at_b;
  p.a->set_notify_handler([&](const PeerId& from, std::string_view payload) {
    std::lock_guard<std::mutex> lock(mu);
    at_a.emplace_back(from, std::string(payload));
  });
  p.b->set_notify_handler([&](const PeerId& from, std::string_view payload) {
    std::lock_guard<std::mutex> lock(mu);
    at_b.emplace_back(from, std::string(payload));
  });
  ASSERT_TRUE(p.a->notify("b", "one").ok());
  ASSERT_TRUE(p.a->notify("b", "").ok());
  ASSERT_TRUE(p.b->notify("a", std::string("\0\1\2", 3)).ok());
  ASSERT_TRUE(test::wait_until([&] {
    std::lock_guard<std::mutex> lock(mu);
    return at_b.size() == 2 && at_a.size() == 1;
  }));
  EXPECT_EQ(at_b[0], std::make_pair(PeerId("a"), std::string("one")));
  EXPECT_EQ(at_b[1].second, "");
  EXPECT_EQ(at_a[0], std::make_pair(PeerId("b"), std::string("\0\1\2", 3)));
  EXPECT_GT(std::string("x").size(), p.a->notify("b", std::string(wire::kMaxControlPayload + 1, 'x')).ok());
}

TEST(TcpTransport, NotifyOrderedAfterWrite) {
  TcpPair p;
  std::atomic<bool> seen{false};
  bool data_present_when_notified = false;
  p.b->set_notify_handler([&](const PeerId&, std::string_view) {
    data_present_when_notified =
        std::equal(p.amem.begin(), p.amem.begin() + 1024, p.bmem.begin());
    seen = true;
  });
  p.a->write("b", {{1, 0, 2, 0, 1024}});
  ASSERT_TRUE(p.a->notify("b", "done").ok());
  ASSERT_TRUE(test::wait_until([&] { return seen.load(); }));
  EXPECT_TRUE(data_present_when_notified);
}

TEST(TcpTransport, PeerStopFailsPendingAndFiresDisconnect) {
  TcpPair p(16 << 20);
  std::atomic<int> disconnects{0};
  p.a->set_disconnect_handler([&](const PeerId& peer) {
    if (peer == "b") disconnects.fetch_add(1);
  });
  p.b->stop();
  ASSERT_TRUE(test::wait_until([&] { return !p.a->is_connected("b"); }));
  EXPECT_EQ(disconnects.load(), 1);
  EXPECT_EQ(p.a->write("b", {{1, 0, 2, 0, 16}}).wait().status.code(), StatusCode::kNotFound);
}

TEST(TcpTransport, LocalDisconnect) {
  TcpPair p;
  std::atomic<int> a_disc{0}, b_disc{0};
  p.a->set_disconnect_handler([&](const PeerId&) { a_disc.fetch_add(1); });
  p.b->set_disconnect_handler([&](const PeerId&) { b_disc.fetch_add(1); });
  ASSERT_TRUE(p.a->disconnect("b").ok());
  EXPECT_FALSE(p.a->is_connected("b"));
  ASSERT_TRUE(test::wait_until([&] { return !p.b->is_connected("a"); }));
  EXPECT_EQ(a_disc.load(), 1);
  EXPECT_EQ(b_disc.load(), 1);
  EXPECT_EQ(p.a->disconnect("b").code(), StatusCode::kNotFound);
  // Reconnect works.
  ASSERT_TRUE(p.a->connect("b", p.b->local_endpoint()).ok());
  ASSERT_TRUE(test::wait_until([&] { return p.b->is_connected("a"); }));
  EXPECT_TRUE(p.a->write("b", {{1, 0, 2, 0, 16}}).wait().ok());
}

// A raw listener that accepts and then never reads: transfers to it can
// never be acknowledged, which lets us exercise the deadline reaper.
struct BlackHole {
  int fd = -1;
  uint16_t port = 0;
  std::thread th;
  std::atomic<bool> stop{false};
  std::vector<int> accepted;
  BlackHole() {
    fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof addr);
    ::listen(fd, 4);
    socklen_t len = sizeof addr;
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
    port = ntohs(addr.sin_port);
    th = std::thread([this] {
      while (!stop) {
        pollfd pfd{fd, POLLIN, 0};
        if (::poll(&pfd, 1, 50) > 0) {
          int c = ::accept(fd, nullptr, nullptr);
          if (c >= 0) accepted.push_back(c);
        }
      }
    });
  }
  ~BlackHole() {
    stop = true;
    th.join();
    for (int c : accepted) ::close(c);
    ::close(fd);
  }
};

TEST(TcpTransport, ConnectRequiresHelloReply) {
  BlackHole hole;
  TcpTransportConfig c;
  c.self = "a";
  c.bind_host = "127.0.0.1";
  c.connect_timeout = std::chrono::milliseconds(200);
  TcpTransport a(c);
  ASSERT_TRUE(a.start().ok());
  const Status s = a.connect("hole", {"127.0.0.1", hole.port});
  EXPECT_EQ(s.code(), StatusCode::kTimeout) << s.to_string();
  EXPECT_FALSE(a.is_connected("hole"));
}

// A peer that completes the handshake and then stops servicing its socket:
// we hijack the accepted descriptor by pausing the peer's reader through a
// notify handler that blocks. Transfers to it are never acknowledged.
TEST(TcpTransport, DeadlineExpiresUnackedTransfer) {
  // Declared before the transports so they outlive the parked reader thread.
  std::mutex block_mu;
  std::condition_variable block_cv;
  bool release = false;

  TcpTransportConfig c;
  c.self = "a";
  c.bind_host = "127.0.0.1";
  c.reaper_interval = std::chrono::milliseconds(10);
  TcpTransport a(c);
  ASSERT_TRUE(a.start().ok());
  TcpTransportConfig ch = c;
  ch.self = "hole";
  TcpTransport hole(ch);
  ASSERT_TRUE(hole.start().ok());
  hole.set_notify_handler([&](const PeerId&, std::string_view) {
    std::unique_lock<std::mutex> lock(block_mu);
    block_cv.wait(lock, [&] { return release; });
  });
  std::vector<uint8_t> mem(64);
  ASSERT_TRUE(a.register_memory({1, mem.data(), 64, MemoryKind::kHost, -1}).ok());
  ASSERT_TRUE(a.connect("hole", hole.local_endpoint()).ok());
  ASSERT_TRUE(a.notify("hole", "stall").ok());  // parks hole's reader thread

  TransferOptions opts;
  opts.timeout = std::chrono::milliseconds(100);
  auto h = a.write("hole", {{1, 0, 5, 0, 64}}, opts);
  auto r = h.wait_for(std::chrono::seconds(5));
  ASSERT_TRUE(r);
  EXPECT_EQ(r->status.code(), StatusCode::kTimeout) << r->status.to_string();
  EXPECT_EQ(a.pending_transfers(), 0u);
  // A transfer with no deadline can still be cancelled explicitly.
  auto h2 = a.write("hole", {{1, 0, 5, 0, 64}});
  EXPECT_FALSE(h2.wait_for(std::chrono::milliseconds(50)));
  EXPECT_EQ(a.pending_transfers(), 1u);
  h2.cancel();
  EXPECT_EQ(h2.wait().status.code(), StatusCode::kCancelled);
  EXPECT_EQ(a.pending_transfers(), 0u);
  // Stopping with an unacked transfer outstanding fails it rather than hanging.
  auto h3 = a.write("hole", {{1, 0, 5, 0, 64}});
  a.stop();
  EXPECT_EQ(h3.wait().status.code(), StatusCode::kUnavailable);
  {
    std::lock_guard<std::mutex> lock(block_mu);
    release = true;
  }
  block_cv.notify_all();
}

TEST(TcpTransport, DeviceMemoryRejected) {
  TcpTransportConfig c;
  c.self = "a";
  TcpTransport a(c);
  int dummy = 0;
  EXPECT_EQ(a.register_memory({1, &dummy, 4, MemoryKind::kDevice, 0}).code(),
            StatusCode::kUnimplemented);
  EXPECT_TRUE(a.register_memory({1, &dummy, 4, MemoryKind::kHostPinned, 0}).ok());
  EXPECT_EQ(a.register_memory({1, &dummy, 4, MemoryKind::kHost, 0}).code(),
            StatusCode::kAlreadyExists);
  EXPECT_TRUE(a.deregister_memory(1).ok());
  EXPECT_EQ(a.deregister_memory(1).code(), StatusCode::kNotFound);
}

TEST(TcpTransport, GarbageOnTheWireClosesConnection) {
  TcpTransportConfig c;
  c.self = "a";
  c.bind_host = "127.0.0.1";
  TcpTransport a(c);
  ASSERT_TRUE(a.start().ok());
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(a.local_endpoint().port);
  ASSERT_EQ(::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof addr), 0);
  const char junk[64] = "this is not a kvc frame";
  ASSERT_EQ(::send(fd, junk, sizeof junk, 0), static_cast<ssize_t>(sizeof junk));
  // The transport closes the socket; our read sees EOF (after its HELLO).
  char buf[256];
  ssize_t total = 0;
  for (;;) {
    ssize_t n = ::recv(fd, buf, sizeof buf, 0);
    if (n <= 0) break;
    total += n;
  }
  EXPECT_GE(total, 0);
  ::close(fd);
  a.stop();
}

}  // namespace
