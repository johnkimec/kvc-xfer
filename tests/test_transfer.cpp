#include "kvc/transfer.hpp"

#include <gtest/gtest.h>

#include <thread>

using namespace kvc;

TEST(Transfer, CompleteOnce) {
  auto st = std::make_shared<TransferState>(1, "p", 10, std::nullopt);
  TransferHandle h(st);
  EXPECT_FALSE(h.done());
  EXPECT_EQ(h.expected_bytes(), 10u);
  int calls = 0;
  h.on_complete([&](const TransferResult& r) {
    ++calls;
    EXPECT_TRUE(r.ok());
    EXPECT_EQ(r.bytes, 10u);
  });
  EXPECT_TRUE(st->complete(Status::Ok(), 10));
  EXPECT_FALSE(st->complete(Status::Internal("late"), 0));
  EXPECT_TRUE(h.done());
  EXPECT_TRUE(h.wait().ok());
  EXPECT_EQ(calls, 1);
  // Late callbacks fire immediately with the stored result.
  h.on_complete([&](const TransferResult& r) {
    ++calls;
    EXPECT_TRUE(r.ok());
  });
  EXPECT_EQ(calls, 2);
}

TEST(Transfer, WaitForTimesOut) {
  auto st = std::make_shared<TransferState>(2, "p", 0, std::nullopt);
  TransferHandle h(st);
  EXPECT_FALSE(h.wait_for(std::chrono::milliseconds(20)));
  std::thread t([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    st->complete(Status::Ok(), 0);
  });
  auto r = h.wait_for(std::chrono::seconds(5));
  ASSERT_TRUE(r);
  EXPECT_TRUE(r->ok());
  t.join();
}

TEST(Transfer, CancelRunsHookOnce) {
  auto st = std::make_shared<TransferState>(3, "p", 0, std::nullopt);
  int hook_calls = 0;
  st->set_cancel_hook([&] { ++hook_calls; });
  TransferHandle h(st);
  h.cancel();
  h.cancel();
  EXPECT_EQ(hook_calls, 1);
  EXPECT_EQ(h.wait().status.code(), StatusCode::kCancelled);
  // Cancelling a finished transfer is a no-op.
  auto done = std::make_shared<TransferState>(4, "p", 0, std::nullopt);
  done->complete(Status::Ok(), 1);
  done->set_cancel_hook([&] { ++hook_calls; });
  done->cancel();
  EXPECT_EQ(hook_calls, 1);
  EXPECT_TRUE(done->wait().ok());
}

TEST(Transfer, FailedHandle) {
  auto h = TransferHandle::failed(Status::NotFound("nope"), "peer");
  EXPECT_TRUE(h.valid());
  EXPECT_TRUE(h.done());
  EXPECT_EQ(h.peer(), "peer");
  EXPECT_EQ(h.wait().status.code(), StatusCode::kNotFound);
  EXPECT_NE(h.id(), TransferHandle::failed(Status::Internal("x")).id());
  TransferHandle empty;
  EXPECT_FALSE(empty.valid());
  EXPECT_TRUE(empty.done());
  EXPECT_EQ(empty.wait().status.code(), StatusCode::kInternal);
}
