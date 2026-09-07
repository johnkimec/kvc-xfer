#include "kvc/protocol.hpp"

#include <gtest/gtest.h>

using namespace kvc;
using namespace kvc::wire;

TEST(Protocol, WriterReaderRoundTrip) {
  Writer w;
  w.u8(7).u16(0xBEEF).u32(0xDEADBEEF).u64(0x0123456789ABCDEFull).str("hello");
  ASSERT_EQ(w.size(), 1u + 2 + 4 + 8 + 4 + 5);
  Reader r(w.data());
  uint8_t a;
  uint16_t b;
  uint32_t c;
  uint64_t d;
  std::string s;
  ASSERT_TRUE(r.u8(a));
  ASSERT_TRUE(r.u16(b));
  ASSERT_TRUE(r.u32(c));
  ASSERT_TRUE(r.u64(d));
  ASSERT_TRUE(r.str(s));
  EXPECT_EQ(a, 7);
  EXPECT_EQ(b, 0xBEEF);
  EXPECT_EQ(c, 0xDEADBEEFu);
  EXPECT_EQ(d, 0x0123456789ABCDEFull);
  EXPECT_EQ(s, "hello");
  EXPECT_TRUE(r.at_end());
  EXPECT_FALSE(r.u8(a));  // underflow is reported, not UB
}

TEST(Protocol, ReaderRejectsOversizedString) {
  Writer w;
  w.u32(1000).bytes("abc", 3);
  Reader r(w.data());
  std::string s;
  EXPECT_FALSE(r.str(s));
  Writer w2;
  w2.str("abcdef");
  Reader r2(w2.data());
  EXPECT_FALSE(r2.str(s, 3));
}

TEST(Protocol, FrameHeaderRoundTrip) {
  uint8_t buf[kFrameHeaderSize];
  encode_frame_header({FrameType::kWrite, 42, 1234567}, buf);
  auto h = decode_frame_header(buf);
  ASSERT_TRUE(h.ok());
  EXPECT_EQ(h->type, FrameType::kWrite);
  EXPECT_EQ(h->transfer_id, 42u);
  EXPECT_EQ(h->payload_length, 1234567u);
  EXPECT_EQ(buf[0], 'K');
  EXPECT_EQ(buf[1], 'V');
  EXPECT_EQ(buf[2], 'C');
  EXPECT_EQ(buf[3], '1');
}

TEST(Protocol, FrameHeaderErrors) {
  uint8_t buf[kFrameHeaderSize];
  encode_frame_header({FrameType::kNotify, 1, 2}, buf);
  buf[0] ^= 0xff;
  EXPECT_EQ(decode_frame_header(buf).status().code(), StatusCode::kProtocolError);
  encode_frame_header({FrameType::kNotify, 1, 2}, buf);
  buf[4] = 99;  // version
  EXPECT_EQ(decode_frame_header(buf).status().code(), StatusCode::kProtocolError);
  encode_frame_header({FrameType::kNotify, 1, 2}, buf);
  buf[6] = 0;  // type 0
  EXPECT_FALSE(decode_frame_header(buf).ok());
  buf[6] = 200;
  EXPECT_FALSE(decode_frame_header(buf).ok());
}

TEST(Protocol, ChunkAndReadDescRoundTrip) {
  uint8_t c[kChunkHeaderSize];
  encode_chunk_header({9, 1ull << 40, 77}, c);
  auto ch = decode_chunk_header(c);
  EXPECT_EQ(ch.region, 9u);
  EXPECT_EQ(ch.offset, 1ull << 40);
  EXPECT_EQ(ch.length, 77u);

  uint8_t d[kReadDescSize];
  encode_read_desc({1, 2, 3, 4, 5}, d);
  EXPECT_EQ(decode_read_desc(d), (XferDesc{1, 2, 3, 4, 5}));
}
