#include "kvc/protocol.hpp"

#include <bit>

static_assert(std::endian::native == std::endian::little,
              "kvc wire codec assumes a little-endian host");

namespace kvc::wire {

namespace {
template <typename T>
void put(uint8_t*& out, T v) {
  std::memcpy(out, &v, sizeof(T));
  out += sizeof(T);
}
template <typename T>
T get(const uint8_t*& in) {
  T v;
  std::memcpy(&v, in, sizeof(T));
  in += sizeof(T);
  return v;
}
}  // namespace

const char* frame_type_name(FrameType type) {
  switch (type) {
    case FrameType::kHello: return "HELLO";
    case FrameType::kWrite: return "WRITE";
    case FrameType::kWriteAck: return "WRITE_ACK";
    case FrameType::kReadReq: return "READ_REQ";
    case FrameType::kReadResp: return "READ_RESP";
    case FrameType::kNotify: return "NOTIFY";
  }
  return "UNKNOWN";
}

void encode_frame_header(const FrameHeader& h, uint8_t* out) {
  put<uint32_t>(out, kMagic);
  put<uint16_t>(out, kVersion);
  put<uint16_t>(out, static_cast<uint16_t>(h.type));
  put<uint64_t>(out, h.transfer_id);
  put<uint64_t>(out, h.payload_length);
}

Result<FrameHeader> decode_frame_header(const uint8_t* in) {
  const uint32_t magic = get<uint32_t>(in);
  if (magic != kMagic) return Status::ProtocolError("bad frame magic");
  const uint16_t version = get<uint16_t>(in);
  if (version != kVersion) {
    return Status::ProtocolError("unsupported protocol version " + std::to_string(version));
  }
  const uint16_t type = get<uint16_t>(in);
  if (type < static_cast<uint16_t>(FrameType::kHello) ||
      type > static_cast<uint16_t>(FrameType::kNotify)) {
    return Status::ProtocolError("unknown frame type " + std::to_string(type));
  }
  FrameHeader h;
  h.type = static_cast<FrameType>(type);
  h.transfer_id = get<uint64_t>(in);
  h.payload_length = get<uint64_t>(in);
  return h;
}

void encode_chunk_header(const ChunkHeader& h, uint8_t* out) {
  put<uint32_t>(out, h.region);
  put<uint64_t>(out, h.offset);
  put<uint64_t>(out, h.length);
}

ChunkHeader decode_chunk_header(const uint8_t* in) {
  ChunkHeader h;
  h.region = get<uint32_t>(in);
  h.offset = get<uint64_t>(in);
  h.length = get<uint64_t>(in);
  return h;
}

void encode_read_desc(const XferDesc& d, uint8_t* out) {
  put<uint32_t>(out, d.src_region);
  put<uint64_t>(out, d.src_offset);
  put<uint32_t>(out, d.dst_region);
  put<uint64_t>(out, d.dst_offset);
  put<uint64_t>(out, d.length);
}

XferDesc decode_read_desc(const uint8_t* in) {
  XferDesc d;
  d.src_region = get<uint32_t>(in);
  d.src_offset = get<uint64_t>(in);
  d.dst_region = get<uint32_t>(in);
  d.dst_offset = get<uint64_t>(in);
  d.length = get<uint64_t>(in);
  return d;
}

Writer& Writer::u8(uint8_t v) {
  buf_.push_back(v);
  return *this;
}
Writer& Writer::u16(uint16_t v) { return bytes(&v, sizeof v); }
Writer& Writer::u32(uint32_t v) { return bytes(&v, sizeof v); }
Writer& Writer::u64(uint64_t v) { return bytes(&v, sizeof v); }
Writer& Writer::bytes(const void* data, size_t len) {
  const auto* p = static_cast<const uint8_t*>(data);
  buf_.insert(buf_.end(), p, p + len);
  return *this;
}
Writer& Writer::str(std::string_view s) {
  u32(static_cast<uint32_t>(s.size()));
  return bytes(s.data(), s.size());
}

bool Reader::u8(uint8_t& v) { return bytes(&v, sizeof v); }
bool Reader::u16(uint16_t& v) { return bytes(&v, sizeof v); }
bool Reader::u32(uint32_t& v) { return bytes(&v, sizeof v); }
bool Reader::u64(uint64_t& v) { return bytes(&v, sizeof v); }
bool Reader::bytes(void* out, size_t len) {
  if (remaining() < len) return false;
  std::memcpy(out, cur_, len);
  cur_ += len;
  return true;
}
bool Reader::str(std::string& out, size_t max_len) {
  uint32_t len = 0;
  if (!u32(len)) return false;
  if (len > max_len || remaining() < len) return false;
  out.assign(reinterpret_cast<const char*>(cur_), len);
  cur_ += len;
  return true;
}
bool Reader::skip(size_t len) {
  if (remaining() < len) return false;
  cur_ += len;
  return true;
}

}  // namespace kvc::wire
