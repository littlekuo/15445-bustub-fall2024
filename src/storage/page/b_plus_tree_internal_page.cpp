//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_internal_page.cpp
//
// Identification: src/storage/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new internal page.
 *
 * Writes the necessary header information to a newly created page,
 * including set page type, set current size, set page id, set parent id and set max page size,
 * must be called after the creation of a new page to make a valid BPlusTreeInternalPage.
 *
 * @param max_size Maximal size of the page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  BUSTUB_ASSERT(max_size <= INTERNAL_PAGE_SLOT_CNT, "max size exceeds internal page slot count");
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  page_id_ = INVALID_PAGE_ID;
}

/**
 * @brief Helper method to get/set the key associated with input "index"(a.k.a
 * array offset).
 *
 * @param index The index of the key to get. Index must be non-zero.
 * @return Key at index
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  BUSTUB_ASSERT(index > 0 && index < GetSize(), "Index out of bounds for key access");
  return key_array_[index];
}

/**
 * @brief Set key at the specified index.
 *
 * @param index The index of the key to set. Index must be non-zero.
 * @param key The new value for key
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  BUSTUB_ASSERT(index > 0 && index < GetSize(), "Index out of bounds for key access");
  key_array_[index] = key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "Index out of bounds for value access");
  page_id_array_[index] = value;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAt(int index) {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "Index out of bounds for value access");
  for (int i = index; i < GetSize() - 1; i++) {
    page_id_array_[i] = page_id_array_[i + 1];
    key_array_[i] = key_array_[i + 1];
  }
  ChangeSizeBy(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (page_id_array_[i] == value) {
      return i;
    }
  }
  return -1;
}

/**
 * @brief Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 *
 * @param index The index of the value to get.
 * @return Value at index
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "Index out of bounds for value access");
  return page_id_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetPageId() const -> page_id_t { return page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetPageId(page_id_t page_id) { page_id_ = page_id; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::PageIndexByKey(const KeyType &key, const KeyComparator &comparator) const -> int {
  auto it = std::upper_bound(key_array_ + 1, key_array_ + GetSize(), key,
                             [&](const KeyType &a, const KeyType &b) { return comparator(a, b) < 0; });
  return it - key_array_ - 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator,
                                            std::vector<std::pair<KeyType, ValueType>> &redistributions) -> bool {
  auto idx = std::lower_bound(key_array_ + 1, key_array_ + GetSize(), key,
                              [&](const auto &a, const auto &b) { return comparator(a, b) < 0; }) -
             key_array_;
  if (GetSize() < GetMaxSize()) {
    if (idx != GetSize()) {
      BUSTUB_ASSERT(comparator(key, key_array_[idx]) != 0, "should not be duplicated");
    }
    for (int i = GetSize(); i >= idx; i--) {
      if (i == idx) {
        key_array_[i] = key;
        page_id_array_[i] = value;
        continue;
      }
      key_array_[i] = key_array_[i - 1];
      page_id_array_[i] = page_id_array_[i - 1];
    }
    ChangeSizeBy(1);
    return true;
  }

  for (int i = GetMaxSize(); i >= 0; i--) {
    if (i >= (GetMaxSize() + 1) / 2) {
      if (i == idx) {
        redistributions.emplace_back(key, value);
      } else if (i > idx) {
        redistributions.emplace_back(key_array_[i - 1], page_id_array_[i - 1]);
      } else {
        redistributions.emplace_back(key_array_[i], page_id_array_[i]);
      }
      continue;
    }
    if (i == idx) {
      key_array_[i] = key;
      page_id_array_[i] = value;
    } else if (i > idx) {
      key_array_[i] = key_array_[i - 1];
      page_id_array_[i] = page_id_array_[i - 1];
    }
  }
  std::reverse(redistributions.begin(), redistributions.end());
  ChangeSizeBy(-(GetMaxSize() / 2));
  return true;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
