#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include "kvc/status.hpp"
#include "kvc/types.hpp"

// Wire format shared by network transports. All integers little-endian.
//
// Frame:  FrameHeader (24 bytes) followed by `payload_length` bytes.
//   u32 magic | u16 version | u16 type | u64 transfer_id | u64 payload_length
//
// kHello     payload: u32 flags, str peer_id
// kWrite     payload: u32 nchunks, then nchunks x { ChunkHeader, data }
// kWriteAck  payload: u32 status, u64 bytes, u32 msg_len, msg
// kReadReq   payload: u32 nchunks, then nchunks x ReadDesc
// kReadResp  payload: u32 status, u32 nchunks, u32 msg_len, msg,
//                     then nchunks x { ChunkHeader, data }
// kNotify    payload: opaque bytes
//
// ChunkHeader: u32 region | u64 offset | u64 length        (20 bytes)
// ReadDesc:    u32 src_region | u64 src_offset | u32 dst_region | u64 dst_offset | u64 length (32 bytes)
// str:         u32 length | bytes

namespace kvc::wire {

constexpr uint32_t kMagic = 0x3143564Bu;  // "KVC1" when read as little-endian bytes
constexpr uint16_t kVersion = 1;

constexpr size_t kFrameHeaderSize = 24;
constexpr size_t kChunkHeaderSize = 20;
constexpr size_t kReadDescSize = 32;

// Guardrails for control payloads that are buffered in full before parsing.
constexpr uint64_t kMaxControlPayload = 16ull * 1024 * 1024;
constexpr uint32_t kMaxChunks = 1u << 20;

enum class FrameType : uint16_t {
  kHello = 1,
  kWrite = 2,
  kWriteAck = 3,
  kReadReq = 4,
  kReadResp = 5,
  kNotify = 6,
};

const char* frame_type_name(FrameType type);

struct FrameHeader {
  FrameType type = FrameType::kHello;
  TransferId transfer_id = 0;
  uint64_t payload_length = 0;
};

void encode_frame_header(const FrameHeader& header, uint8_t* out);
Result<FrameHeader> decode_frame_header(const uint8_t* in);

struct ChunkHeader {
  RegionId region = kInvalidRegion;
  uint64_t offset = 0;
  uint64_t length = 0;
};

void encode_chunk_header(const ChunkHeader& header, uint8_t* out);
ChunkHeader decode_chunk_header(const uint8_t* in);

void encode_read_desc(const XferDesc& desc, uint8_t* out);
XferDesc decode_read_desc(const uint8_t* in);

// Append-only little-endian serializer.
class Writer {
 public:
  Writer& u8(uint8_t v);
  Writer& u16(uint16_t v);
  Writer& u32(uint32_t v);
  Writer& u64(uint64_t v);
  Writer& bytes(const void* data, size_t len);
  Writer& str(std::string_view s);  // u32 length prefix

  std::vector<uint8_t>& data() { return buf_; }
  const std::vector<uint8_t>& data() const { return buf_; }
  size_t size() const { return buf_.size(); }
  void reserve(size_t n) { buf_.reserve(n); }

 private:
  std::vector<uint8_t> buf_;
};

// Bounds-checked deserializer; every accessor returns false on underflow.
class Reader {
 public:
  Reader(const uint8_t* data, size_t len) : cur_(data), end_(data + len) {}
  explicit Reader(std::string_view s)
      : Reader(reinterpret_cast<const uint8_t*>(s.data()), s.size()) {}
  explicit Reader(const std::vector<uint8_t>& v) : Reader(v.data(), v.size()) {}

  bool u8(uint8_t& v);
  bool u16(uint16_t& v);
  bool u32(uint32_t& v);
  bool u64(uint64_t& v);
  bool bytes(void* out, size_t len);
  bool str(std::string& out, size_t max_len = kMaxControlPayload);
  bool skip(size_t len);

  size_t remaining() const { return static_cast<size_t>(end_ - cur_); }
  bool at_end() const { return cur_ == end_; }
  const uint8_t* cursor() const { return cur_; }

 private:
  const uint8_t* cur_;
  const uint8_t* end_;
};

}  // namespace kvc::wire
