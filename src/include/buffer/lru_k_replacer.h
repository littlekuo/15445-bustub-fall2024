//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>  // offsetof
#include <cstdint>
#include <deque>
#include <future>  // NOLINT
#include <limits>
#include <map>
#include <memory>
#include <mutex>  // NOLINT
#include <new>    // std::hardware_destructive_interference_size
#include <optional>
#include <thread>  // NOLINT
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

// the following import from file src/include/common/mpmc.h

#ifndef __cpp_aligned_new
#ifdef _WIN32
#include <malloc.h>  // _aligned_malloc
#else
#include <stdlib.h>  // posix_memalign
#endif
#endif

namespace rigtorp {
namespace mpmc {
#if defined(__cpp_lib_hardware_interference_size) && !defined(__APPLE__)
static constexpr size_t HARDWARE_INTERFERENCE_SIZE = std::hardware_destructive_interference_size;
#else
static constexpr size_t HARDWARE_INTERFERENCE_SIZE = 64;
#endif

#if defined(__cpp_aligned_new)
template <typename T>
using AlignedAllocator = std::allocator<T>;
#else
template <typename T>
struct AlignedAllocator {
  using value_type = T;

  T *allocate(std::size_t n) {
    if (n > std::numeric_limits<std::size_t>::max() / sizeof(T)) {
      throw std::bad_array_new_length();
    }
#ifdef _WIN32
    auto *p = static_cast<T *>(_aligned_malloc(sizeof(T) * n, alignof(T)));
    if (p == nullptr) {
      throw std::bad_alloc();
    }
#else
    T *p;
    if (posix_memalign(reinterpret_cast<void **>(&p), alignof(T), sizeof(T) * n) != 0) {
      throw std::bad_alloc();
    }
#endif
    return p;
  }

  void deallocate(T *p, std::size_t) {
#ifdef _WIN32
    _aligned_free(p);
#else
    free(p);
#endif
  }
};
#endif

template <typename T>
struct Slot {
  ~Slot() noexcept {
    if ((turn_ & 1) != 0U) {
      Destroy();
    }
  }

  template <typename... Args>
  void Construct(Args &&...args) noexcept {
    static_assert(std::is_nothrow_constructible<T, Args &&...>::value,
                  "T must be nothrow constructible with Args&&...");
    new (&storage_) T(std::forward<Args>(args)...);
  }

  void Destroy() noexcept {
    static_assert(std::is_nothrow_destructible<T>::value, "T must be nothrow destructible");
    reinterpret_cast<T *>(&storage_)->~T();
  }

  auto Move() noexcept -> T && { return reinterpret_cast<T &&>(storage_); }

  // Align to avoid false sharing between adjacent slots
  alignas(HARDWARE_INTERFERENCE_SIZE) std::atomic<size_t> turn_ = {0};
  typename std::aligned_storage<sizeof(T), alignof(T)>::type storage_;
};

template <typename T, typename Allocator = AlignedAllocator<Slot<T>>>
class Queue {  // NOLINT
 private:
  static_assert(std::is_nothrow_copy_assignable<T>::value || std::is_nothrow_move_assignable<T>::value,
                "T must be nothrow copy or move assignable");

  static_assert(std::is_nothrow_destructible<T>::value, "T must be nothrow destructible");

 public:
  explicit Queue(const size_t capacity, const Allocator &allocator = Allocator())
      : capacity_(capacity), allocator_(allocator), head_(0), tail_(0) {
    if (capacity_ < 1) {
      throw std::invalid_argument("capacity < 1");
    }
    // Allocate one extra slot to prevent false sharing on the last slot
    slots_ = allocator_.allocate(capacity_ + 1);
    // Allocators are not required to honor alignment for over-aligned types
    // (see http://eel.is/c++draft/allocator.requirements#10) so we verify
    // alignment here
    if (reinterpret_cast<size_t>(slots_) % alignof(Slot<T>) != 0) {
      allocator_.deallocate(slots_, capacity_ + 1);
      throw std::bad_alloc();
    }
    for (size_t i = 0; i < capacity_; ++i) {
      new (&slots_[i]) Slot<T>();
    }
    static_assert(alignof(Slot<T>) == HARDWARE_INTERFERENCE_SIZE,
                  "Slot must be aligned to cache line boundary to prevent false sharing");
    static_assert(sizeof(Slot<T>) % HARDWARE_INTERFERENCE_SIZE == 0,
                  "Slot size must be a multiple of cache line size to prevent "
                  "false sharing between adjacent slots");
    static_assert(sizeof(Queue) % HARDWARE_INTERFERENCE_SIZE == 0,
                  "Queue size must be a multiple of cache line size to "
                  "prevent false sharing between adjacent queues");
    static_assert(
        offsetof(Queue, tail_) - offsetof(Queue, head_) == static_cast<std::ptrdiff_t>(HARDWARE_INTERFERENCE_SIZE),
        "head and tail must be a cache line apart to prevent false sharing");
  }

  ~Queue() noexcept {
    for (size_t i = 0; i < capacity_; ++i) {
      slots_[i].~Slot();
    }
    allocator_.deallocate(slots_, capacity_ + 1);
  }

  // non-copyable and non-movable
  Queue(const Queue &) = delete;
  auto operator=(const Queue &) -> Queue & = delete;

  template <typename... Args>
  void Emplace(Args &&...args) noexcept {
    static_assert(std::is_nothrow_constructible<T, Args &&...>::value,
                  "T must be nothrow constructible with Args&&...");
    auto const head = head_.fetch_add(1);
    auto &slot = slots_[Idx(head)];
    while (Turn(head) * 2 != slot.turn_.load(std::memory_order_acquire)) {
    }
    slot.Construct(std::forward<Args>(args)...);
    slot.turn_.store(Turn(head) * 2 + 1, std::memory_order_release);
  }

  template <typename... Args>
  auto TryEmplace(Args &&...args) noexcept -> bool {
    static_assert(std::is_nothrow_constructible<T, Args &&...>::value,
                  "T must be nothrow constructible with Args&&...");
    auto head = head_.load(std::memory_order_acquire);
    for (;;) {
      auto &slot = slots_[Idx(head)];
      if (Turn(head) * 2 == slot.turn_.load(std::memory_order_acquire)) {
        if (head_.compare_exchange_strong(head, head + 1)) {
          slot.construct(std::forward<Args>(args)...);
          slot.turn.store(Turn(head) * 2 + 1, std::memory_order_release);
          return true;
        }
      } else {
        auto const prev_head = head;
        head = head_.load(std::memory_order_acquire);
        if (head == prev_head) {
          return false;
        }
      }
    }
  }

  void Push(const T &v) noexcept {
    static_assert(std::is_nothrow_copy_constructible<T>::value, "T must be nothrow copy constructible");
    Emplace(v);
  }

  template <typename P, typename = typename std::enable_if<std::is_nothrow_constructible<T, P &&>::value>::type>
  void Push(P &&v) noexcept {
    Emplace(std::forward<P>(v));
  }

  auto TryPush(const T &v) noexcept -> bool {
    static_assert(std::is_nothrow_copy_constructible<T>::value, "T must be nothrow copy constructible");
    return TryEmplace(v);
  }

  template <typename P, typename = typename std::enable_if<std::is_nothrow_constructible<T, P &&>::value>::type>
  auto TryPush(P &&v) noexcept -> bool {
    return TryEmplace(std::forward<P>(v));
  }

  void Pop(T &v) noexcept {
    auto const tail = tail_.fetch_add(1);
    auto &slot = slots_[Idx(tail)];
    while (Turn(tail) * 2 + 1 != slot.turn_.load(std::memory_order_acquire)) {
    }
    v = slot.Move();
    slot.Destroy();
    slot.turn.store(Turn(tail) * 2 + 2, std::memory_order_release);
  }

  auto TryPop(T &v) noexcept -> bool {
    auto tail = tail_.load(std::memory_order_acquire);
    for (;;) {
      auto &slot = slots_[Idx(tail)];
      if (Turn(tail) * 2 + 1 == slot.turn_.load(std::memory_order_acquire)) {
        if (tail_.compare_exchange_strong(tail, tail + 1)) {
          v = slot.Move();
          slot.Destroy();
          slot.turn_.store(Turn(tail) * 2 + 2, std::memory_order_release);
          return true;
        }
      } else {
        auto const prev_tail = tail;
        tail = tail_.load(std::memory_order_acquire);
        if (tail == prev_tail) {
          return false;
        }
      }
    }
  }

  /// Returns the number of elements in the queue.
  /// The size can be negative when the queue is empty and there is at least one
  /// reader waiting. Since this is a concurrent queue the size is only a best
  /// effort guess until all reader and writer threads have been joined.
  auto Size() const noexcept -> ptrdiff_t {
    return static_cast<ptrdiff_t>(head_.load(std::memory_order_relaxed) - tail_.load(std::memory_order_relaxed));
  }

  /// Returns true if the queue is empty.
  /// Since this is a concurrent queue this is only a best effort guess
  /// until all reader and writer threads have been joined.
  auto Empty() const noexcept -> bool { return Size() <= 0; }

 private:
  constexpr auto Idx(size_t i) const noexcept -> size_t { return i % capacity_; }

  constexpr auto Turn(size_t i) const noexcept -> size_t { return i / capacity_; }

 private:
  const size_t capacity_;
  Slot<T> *slots_;
#if defined(__has_cpp_attribute) && __has_cpp_attribute(no_unique_address)
  Allocator allocator_ [[no_unique_address]];  // NOLINT
#else
  Allocator allocator_;
#endif
  // Align to avoid false sharing between head_ and tail_
  alignas(HARDWARE_INTERFERENCE_SIZE) std::atomic<size_t> head_;
  alignas(HARDWARE_INTERFERENCE_SIZE) std::atomic<size_t> tail_;
};
}  // namespace mpmc

template <typename T, typename Allocator = mpmc::AlignedAllocator<mpmc::Slot<T>>>
using MPMCQueue = mpmc::Queue<T, Allocator>;

}  // namespace rigtorp

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

struct RecordAccessCmd {
  frame_id_t fid_;
};
struct SetEvictableCmd {
  frame_id_t fid_;
  bool is_evictable_;
};
struct RemoveCmd {
  frame_id_t fid_;
};
struct EvictCmd {
  std::promise<std::optional<frame_id_t>> promise_;
};
struct RecordAccessAndSetEvictableCmd {
  frame_id_t fid_;
  bool is_evictable_;
};
using ReplacerCmd = std::variant<RecordAccessCmd, SetEvictableCmd, RemoveCmd, EvictCmd, RecordAccessAndSetEvictableCmd>;

enum class Mode { Synchronous, Asynchronous };

constexpr uint64_t CACHED_FLAG_MASK = uint64_t{1} << 63;

class LRUKNode {
 private:
  /** History of last seen K timestamps of this page. Least recent timestamp stored in front. */
  // Remove maybe_unused if you start using them. Feel free to change the member variables as you want.

  std::deque<uint64_t> history_;
  size_t k_;
  frame_id_t fid_;
  bool is_evictable_{false};

 public:
  LRUKNode() = default;
  LRUKNode(size_t k, frame_id_t fid) : k_(k), fid_(fid) {}
  ~LRUKNode() = default;

  void RecordAccess(uint64_t timestamp) {
    BUSTUB_ASSERT(timestamp < CACHED_FLAG_MASK, "Timestamp exceeds maximum allowed value");
    history_.push_back(timestamp);
    if (history_.size() > k_) {
      history_.pop_front();
    }
  }
  auto GetHistorySize() const -> size_t { return history_.size(); }
  void SetEvictable(bool set_evictable) { is_evictable_ = set_evictable; }
  auto GetPackTimestamp() const -> uint64_t {
    auto timestamp = history_.front();
    if (history_.size() >= k_) {
      timestamp |= CACHED_FLAG_MASK;
    }
    return timestamp;
  }
  auto IsEvictable() const -> bool { return is_evictable_; }
  auto GetFrameId() const -> frame_id_t { return fid_; }
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
  using MPMCQueue = rigtorp::MPMCQueue<std::optional<ReplacerCmd>>;

 public:
  explicit LRUKReplacer(size_t num_frames, size_t k, Mode mode = Mode::Synchronous, size_t num_shards = 256);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer();

  auto Evict() -> std::optional<frame_id_t>;

  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  void Remove(frame_id_t frame_id);

  void RecordAccessAndSetEvictable(frame_id_t frame_id, bool set_evictable);

  auto Size() -> size_t;

 private:
  struct Shard {
    explicit Shard(size_t k) : k_(k) {}
    ~Shard() = default;

    size_t curr_size_{0};
    size_t k_;
    uint64_t current_timestamp_{0};

    std::unordered_map<frame_id_t, LRUKNode> node_store_;
    // bit | timestamp, bit = 0 means less than k
    std::map<uint64_t, frame_id_t> cache_frames_;
    std::unordered_map<frame_id_t, std::map<uint64_t, frame_id_t>::iterator> cache_finder_;

    auto Evict() -> std::optional<frame_id_t>;

    void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

    auto SetEvictable(frame_id_t frame_id, bool set_evictable) -> int;

    auto Remove(frame_id_t frame_id) -> bool;
    auto RecordAcessAndSetEvictable(frame_id_t frame_id, bool set_evictable) -> int;
  };

  void Schedule(std::optional<ReplacerCmd> cmd) { cmd_queue_->Push(std::move(cmd)); }

  void ConsumerLoop(const std::unique_ptr<MPMCQueue> &chan);

  std::vector<std::unique_ptr<Shard>> shards_;
  std::vector<std::unique_ptr<std::mutex>> latches_;
  size_t shard_count_;

  std::unique_ptr<MPMCQueue> cmd_queue_;
  std::thread consumer_thread_;
  std::atomic<size_t> cur_size_{0};
  std::atomic<size_t> next_shard_idx_{0};
  size_t command_count_{0};
  size_t replacer_size_;
  Mode mode_;
};

}  // namespace bustub
