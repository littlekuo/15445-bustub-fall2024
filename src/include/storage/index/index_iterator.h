//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.h
//
// Identification: src/include/storage/index/index_iterator.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <utility>
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

constexpr int INVALID_INDEX = -1;

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator(IndexIterator &&it) noexcept {
    bpm_ = it.bpm_;
    index_in_page_ = it.index_in_page_;
    guard_ = std::move(it.guard_);

    it.index_in_page_ = INVALID_INDEX;
    it.bpm_ = nullptr;
  }

  auto operator=(IndexIterator &&it) noexcept -> IndexIterator & {
    if (this != &it) {
      bpm_ = it.bpm_;
      index_in_page_ = it.index_in_page_;
      guard_ = std::move(it.guard_);

      it.index_in_page_ = INVALID_INDEX;
      it.bpm_ = nullptr;
    }
    return *this;
  }

  IndexIterator();

  IndexIterator(BufferPoolManager *bpm, int index_in_page, ReadPageGuard guard);

  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> std::pair<const KeyType &, const ValueType &>;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    if (index_in_page_ == INVALID_INDEX && itr.index_in_page_ == INVALID_INDEX) {
      return true;
    }
    if (index_in_page_ == INVALID_INDEX || itr.index_in_page_ == INVALID_INDEX) {
      return false;
    }
    return index_in_page_ == itr.index_in_page_ && guard_.GetPageId() == itr.guard_.GetPageId();
  }

  auto operator!=(const IndexIterator &itr) const -> bool { return !(*this == itr); }

  auto IsDeleted() -> bool;

  void Advance();

 private:
  BufferPoolManager *bpm_;
  int index_in_page_{INVALID_INDEX};
  ReadPageGuard guard_;
};

}  // namespace bustub
