#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>  // offsetof
#include <limits>
#include <memory>
#include <new>  // std::hardware_destructive_interference_size
#include <stdexcept>
#include <utility>

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
