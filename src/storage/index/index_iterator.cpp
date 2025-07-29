//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.cpp
//
// Identification: src/storage/index/index_iterator.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/**
 * @note you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, int index_in_page, ReadPageGuard guard)
    : bpm_(bpm), index_in_page_(index_in_page), guard_(std::move(guard)) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return index_in_page_ == INVALID_INDEX; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> {
  auto page = guard_.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  return {page->Key(index_in_page_), page->Value(index_in_page_)};
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (index_in_page_ == INVALID_INDEX) {
    return *this;
  }

  index_in_page_++;

  Advance();
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsDeleted() -> bool {
  auto page = guard_.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  return page->DeletedAt(index_in_page_);
}

INDEX_TEMPLATE_ARGUMENTS
void INDEXITERATOR_TYPE::Advance() {
  if (index_in_page_ == INVALID_INDEX) {
    return;
  }

  while (true) {
    auto page = guard_.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();

    if (index_in_page_ >= page->GetSize()) {
      page_id_t next_page_id = page->GetNextPageId();

      if (next_page_id == INVALID_PAGE_ID) {
        index_in_page_ = INVALID_INDEX;
        guard_.Drop();
        return;
      }

      guard_ = bpm_->ReadPage(next_page_id);
      index_in_page_ = 0;
      continue;
    }

    if (!IsDeleted()) {
      break;
    }
    index_in_page_++;
  }
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
