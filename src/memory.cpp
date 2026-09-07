#include "kvc/memory.hpp"

namespace kvc {

Result<RegionId> MemoryRegistry::add(void* base, uint64_t length, MemoryKind kind, int device) {
  if (base == nullptr) return Status::InvalidArgument("region base is null");
  if (length == 0) return Status::InvalidArgument("region length is zero");
  std::lock_guard<std::mutex> lock(mu_);
  while (regions_.count(next_id_) != 0 || next_id_ == kInvalidRegion) ++next_id_;
  const RegionId id = next_id_++;
  regions_[id] = MemoryRegion{id, base, length, kind, device};
  return id;
}

Status MemoryRegistry::add(const MemoryRegion& region) {
  if (region.id == kInvalidRegion) return Status::InvalidArgument("invalid region id");
  if (region.base == nullptr) return Status::InvalidArgument("region base is null");
  if (region.length == 0) return Status::InvalidArgument("region length is zero");
  std::lock_guard<std::mutex> lock(mu_);
  if (regions_.count(region.id) != 0) {
    return Status::AlreadyExists("region " + std::to_string(region.id) + " already registered");
  }
  regions_[region.id] = region;
  return Status::Ok();
}

Status MemoryRegistry::remove(RegionId id) {
  std::lock_guard<std::mutex> lock(mu_);
  if (regions_.erase(id) == 0) {
    return Status::NotFound("region " + std::to_string(id) + " not registered");
  }
  return Status::Ok();
}

std::optional<MemoryRegion> MemoryRegistry::get(RegionId id) const {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = regions_.find(id);
  if (it == regions_.end()) return std::nullopt;
  return it->second;
}

std::vector<MemoryRegion> MemoryRegistry::list() const {
  std::lock_guard<std::mutex> lock(mu_);
  std::vector<MemoryRegion> out;
  out.reserve(regions_.size());
  for (const auto& [_, r] : regions_) out.push_back(r);
  return out;
}

size_t MemoryRegistry::size() const {
  std::lock_guard<std::mutex> lock(mu_);
  return regions_.size();
}

}  // namespace kvc
