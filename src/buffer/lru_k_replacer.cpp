//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "buffer/lru_k_replacer.h"
#include <algorithm>
#include <chrono>  // NOLINT
#include <thread>  // NOLINT
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

constexpr std::chrono::microseconds MIN_DURATION(128);
constexpr std::chrono::microseconds MAX_DURATION(50000);

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k, Mode mode, size_t num_shards)
    : shard_count_(num_shards), replacer_size_(num_frames), mode_(mode) {
  if (replacer_size_ > 128) {
    mode_ = Mode::Asynchronous;
    shard_count_ = 1;
  }
  shards_.reserve(shard_count_);
  for (size_t i = 0; i < shard_count_; ++i) {
    shards_.emplace_back(std::make_unique<Shard>(k));
  }
  if (mode_ == Mode::Asynchronous) {
    BUSTUB_ASSERT(shard_count_ == 1, "Asynchronous mode only supports one shard");
    cmd_queue_ = std::make_unique<MPMCQueue>(1024);
    consumer_thread_ = std::thread([this]() { ConsumerLoop(cmd_queue_); });
  } else {
    for (size_t i = 0; i < shard_count_; i++) {
      latches_.emplace_back(std::make_unique<std::mutex>());
    }
  }
}

LRUKReplacer::~LRUKReplacer() {
  std::cout << "cnt: " << command_count_ << std::endl;
  if (mode_ == Mode::Asynchronous) {
    cmd_queue_->Push(std::nullopt);
    consumer_thread_.join();
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return true if a frame is evicted successfully, false if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  // std::cout << "Evict" << std::endl;
  size_t start_shard = next_shard_idx_.fetch_add(1) % shard_count_;
  if (mode_ == Mode::Synchronous) {
    for (size_t i = 0; i < shard_count_; ++i) {
      size_t shard_idx = (start_shard + i) % shard_count_;
      std::lock_guard guard(*latches_[shard_idx]);
      auto frame_id = shards_[shard_idx]->Evict();
      if (frame_id.has_value()) {
        cur_size_.fetch_sub(1, std::memory_order_relaxed);
        return frame_id;
      }
    }
    return std::nullopt;
  }

  EvictCmd cmd;
  cmd.promise_ = std::promise<std::optional<frame_id_t>>();
  auto promise = cmd.promise_.get_future();
  Schedule(std::move(cmd));
  auto result = promise.get();
  if (result.has_value()) {
    return result;
  }
  return std::nullopt;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  if (static_cast<size_t>(frame_id) >= replacer_size_ || frame_id < 0) {
    throw bustub::Exception("the frame id is invalid");
  }

  if (mode_ == Mode::Synchronous) {
    size_t shard_idx = frame_id % shard_count_;
    std::lock_guard guard(*latches_[shard_idx]);
    shards_[shard_idx]->RecordAccess(frame_id, access_type);
    return;
  }

  RecordAccessCmd cmd;
  cmd.fid_ = frame_id;
  Schedule(cmd);
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (static_cast<size_t>(frame_id) >= replacer_size_ || frame_id < 0) {
    throw bustub::Exception("the frime id is invalid");
  }
  if (mode_ == Mode::Synchronous) {
    size_t shard_idx = frame_id % shard_count_;
    std::lock_guard guard(*latches_[shard_idx]);
    int ret = shards_[shard_idx]->SetEvictable(frame_id, set_evictable);
    if (ret == 1) {
      cur_size_.fetch_add(1, std::memory_order_relaxed);
    } else if (ret == -1) {
      cur_size_.fetch_sub(1, std::memory_order_relaxed);
    }
    return;
  }

  SetEvictableCmd cmd;
  cmd.fid_ = frame_id;
  cmd.is_evictable_ = set_evictable;
  Schedule(cmd);
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (static_cast<size_t>(frame_id) >= replacer_size_ || frame_id < 0) {
    throw bustub::Exception("the frame id is invalid");
  }
  if (mode_ == Mode::Synchronous) {
    size_t shard_idx = frame_id % shard_count_;
    std::lock_guard guard(*latches_[shard_idx]);
    if (shards_[shard_idx]->Remove(frame_id)) {
      cur_size_.fetch_sub(1, std::memory_order_relaxed);
    }
    return;
  }
  RemoveCmd cmd;
  cmd.fid_ = frame_id;
  Schedule(cmd);
}

void LRUKReplacer::RecordAccessAndSetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (static_cast<size_t>(frame_id) >= replacer_size_ || frame_id < 0) {
    throw bustub::Exception("the frame id is invalid");
  }
  if (mode_ == Mode::Synchronous) {
    size_t shard_idx = frame_id % shard_count_;
    std::lock_guard guard(*latches_[shard_idx]);
    shards_[shard_idx]->RecordAccess(frame_id, AccessType::Unknown);
    shards_[shard_idx]->SetEvictable(frame_id, set_evictable);
    return;
  }
  RecordAccessAndSetEvictableCmd cmd;
  cmd.fid_ = frame_id;
  cmd.is_evictable_ = set_evictable;
  Schedule(cmd);
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t { return cur_size_.load(std::memory_order_relaxed); }

auto LRUKReplacer::Shard::Evict() -> std::optional<frame_id_t> {
  if (curr_size_ <= 0) {
    return std::nullopt;
  }
  auto it = cache_frames_.begin();
  frame_id_t frame_id = it->second;
  cache_frames_.erase(it);
  cache_finder_.erase(frame_id);
  node_store_.erase(frame_id);
  curr_size_--;
  return frame_id;
}

void LRUKReplacer::Shard::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  auto [it, is_new] = node_store_.try_emplace(frame_id, k_, frame_id);
  if (is_new || !it->second.IsEvictable()) {
    it->second.RecordAccess(current_timestamp_++);
    return;
  }

  LRUKNode &node = it->second;
  node.RecordAccess(current_timestamp_++);
  auto finder_it = cache_finder_.find(frame_id);
  if (finder_it != cache_finder_.end()) {
    cache_frames_.erase(finder_it->second);
  }

  auto map_it = cache_frames_.emplace(node.GetPackTimestamp(), frame_id);
  cache_finder_[frame_id] = map_it.first;
}

auto LRUKReplacer::Shard::SetEvictable(frame_id_t frame_id, bool set_evictable) -> int {
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return 0;
  }

  bool origin_evictable = it->second.IsEvictable();
  it->second.SetEvictable(set_evictable);

  if (origin_evictable && !set_evictable) {
    auto finder_it = cache_finder_.find(frame_id);
    if (finder_it != cache_finder_.end()) {
      cache_frames_.erase(finder_it->second);
      cache_finder_.erase(finder_it);
      curr_size_--;
      return -1;
    }
  }

  if (!origin_evictable && set_evictable) {
    auto [iterator, is_new] = cache_frames_.emplace(it->second.GetPackTimestamp(), frame_id);
    cache_finder_[frame_id] = iterator;
    curr_size_++;
    return 1;
  }
  return 0;
}

auto LRUKReplacer::Shard::Remove(frame_id_t frame_id) -> bool {
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return true;
  }
  if (!it->second.IsEvictable()) {
    throw bustub::Exception("the frame is not evictable");
  }
  curr_size_--;

  auto finder_it = cache_finder_.find(frame_id);
  if (finder_it != cache_finder_.end()) {
    cache_frames_.erase(finder_it->second);
    cache_finder_.erase(finder_it);
  }
  node_store_.erase(frame_id);
  return true;
}

auto LRUKReplacer::Shard::RecordAcessAndSetEvictable(frame_id_t frame_id, bool set_evictable) -> int {
  RecordAccess(frame_id);
  return SetEvictable(frame_id, set_evictable);
}

void LRUKReplacer::ConsumerLoop(const std::unique_ptr<rigtorp::MPMCQueue<std::optional<ReplacerCmd>>> &chan) {
  auto current_sleep_duration = MIN_DURATION;
  std::optional<ReplacerCmd> cmd_opt;
  while (true) {
    bool ret = chan->TryPop(cmd_opt);
    if (!ret) {
      //std::cout << "consumer sleep: " << current_sleep_duration.count() << std::endl;
      std::this_thread::sleep_for(current_sleep_duration);
      current_sleep_duration = std::min(current_sleep_duration * 2, MAX_DURATION);
      continue;
    }
    if (!cmd_opt.has_value()) {
      break;
    }
    command_count_++;
    current_sleep_duration = MIN_DURATION;
    std::visit(
        [&](auto &&arg) {
          using T = std::decay_t<decltype(arg)>;
          if constexpr (std::is_same_v<T, RecordAccessCmd>) {  // NOLINT
            shards_[0]->RecordAccess(arg.fid_);
          } else if constexpr (std::is_same_v<T, SetEvictableCmd>) {  // NOLINT
            auto ret = shards_[0]->SetEvictable(arg.fid_, arg.is_evictable_);
            if (ret > 0) {
              cur_size_.fetch_add(ret, std::memory_order_relaxed);
            } else if (ret < 0) {
              cur_size_.fetch_sub(-ret, std::memory_order_relaxed);
            }
          } else if constexpr (std::is_same_v<T, RemoveCmd>) {  // NOLINT
            if (shards_[0]->Remove(arg.fid_)) {
              cur_size_.fetch_sub(1, std::memory_order_relaxed);
            }
          } else if constexpr (std::is_same_v<T, EvictCmd>) {  // NOLINT
            auto ret = shards_[0]->Evict();
            if (ret.has_value()) {
              cur_size_.fetch_sub(1, std::memory_order_relaxed);
            }
            arg.promise_.set_value(ret);
          } else if constexpr (std::is_same_v<T, RecordAccessAndSetEvictableCmd>) {  // NOLINT
            auto ret = shards_[0]->RecordAcessAndSetEvictable(arg.fid_, arg.is_evictable_);
            if (ret > 0) {
              cur_size_.fetch_add(ret, std::memory_order_relaxed);
            } else if (ret < 0) {
              cur_size_.fetch_sub(-ret, std::memory_order_relaxed);
            }
          }
        },
        cmd_opt.value());
  }
}

}  // namespace bustub
