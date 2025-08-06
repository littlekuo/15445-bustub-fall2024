//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_leaf_page.cpp
//
// Identification: src/storage/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new leaf page
 *
 * After creating a new leaf page from buffer pool, must call initialize method to set default values,
 * including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size.
 *
 * @param max_size Max size of the leaf node
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  page_id_ = INVALID_PAGE_ID;
  next_page_id_ = INVALID_PAGE_ID;
  tombstone_cnt_ = 0;
  version_ = 0;
  std::fill(deleted_array_, deleted_array_ + max_size, 0);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index" (a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "Index out of bounds");
  return key_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "Index out of bounds");
  return rid_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::DeletedAt(int index) const -> bool {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "Index out of bounds");
  return deleted_array_[index] != 0;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetPageId() const -> page_id_t { return page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetPageId(page_id_t page_id) { page_id_ = page_id; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyValueAt(int index, const KeyType &key, const ValueType &value) {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "Index out of bounds");
  key_array_[index] = key;
  rid_array_[index] = value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator,
                                        std::vector<std::pair<KeyType, ValueType>> &redistributions, bool is_optimistic)
    -> bool {
  auto pos = std::lower_bound(key_array_, key_array_ + GetSize(), key,
                              [&](const auto &a, const auto &b) { return comparator(a, b) < 0; });
  if (pos != key_array_ + GetSize() && comparator(*pos, key) == 0) {
    if (deleted_array_[pos - key_array_] != 0) {
      deleted_array_[pos - key_array_] = 0;
      tombstone_cnt_--;
      SetKeyValueAt(pos - key_array_, key, value);
      return true;
    }
    return false;
  }

  if (IsFull()) {
    // split
    for (int i = GetMaxSize(); i >= 0; i--) {
      if (i >= (GetMaxSize() + 1) / 2) {
        if (i == pos - key_array_) {
          redistributions.emplace_back(key, value);
        } else if (i > pos - key_array_) {
          redistributions.emplace_back(key_array_[i - 1], rid_array_[i - 1]);
        } else {
          redistributions.emplace_back(key_array_[i], rid_array_[i]);
        }
        continue;
      }
      if (i == pos - key_array_) {
        key_array_[i] = key;
        rid_array_[i] = value;
      } else if (i > pos - key_array_) {
        key_array_[i] = key_array_[i - 1];
        rid_array_[i] = rid_array_[i - 1];
      }
    }
    std::reverse(redistributions.begin(), redistributions.end());
    ChangeSizeBy(-(GetMaxSize() / 2));
    return true;
  }

  // don't split until full when not optimistic
  if (IsOverflow() && !is_optimistic) {
    std::vector<std::pair<KeyType, ValueType>> tmp;
    bool inserted = false;
    for (int i = 0; i < GetSize(); i++) {
      if (deleted_array_[i] != 0) {
        deleted_array_[i] = 0;
        continue;
      }
      if (comparator(key, key_array_[i]) < 0 && !inserted) {
        tmp.emplace_back(key, value);
        inserted = true;
      }
      tmp.emplace_back(key_array_[i], rid_array_[i]);
    }
    if (!inserted) {
      tmp.emplace_back(key, value);
    }
    auto idx = 0;
    for (size_t i = 0; i < tmp.size(); i++) {
      if (i > tmp.size() / 2) {
        redistributions.emplace_back(tmp[i].first, tmp[i].second);
        continue;
      }
      key_array_[idx] = tmp[i].first;
      rid_array_[idx] = tmp[i].second;
      idx++;
    }
    SetSize(idx);
    tombstone_cnt_ = 0;
    return true;
  }

  // size < max_size
  if (GetSize() < GetMaxSize()) {
    auto old_size = GetSize();
    ChangeSizeBy(1);
    for (int i = old_size; i >= pos - key_array_; i--) {
      if (i == pos - key_array_) {
        SetKeyValueAt(i, key, value);
        deleted_array_[i] = 0;
        continue;
      }
      SetKeyValueAt(i, key_array_[i - 1], rid_array_[i - 1]);
      deleted_array_[i] = deleted_array_[i - 1];
    }
    return true;
  }

  // size == max_size but tombstone_cnt > 0
  std::vector<std::pair<KeyType, ValueType>> tmp;
  tmp.reserve(GetSize() - tombstone_cnt_ + 1);
  bool inserted = false;
  for (int i = 0; i < GetSize(); i++) {
    if (deleted_array_[i] != 0) {
      deleted_array_[i] = 0;
      continue;
    }
    if (comparator(key, key_array_[i]) < 0 && !inserted) {
      tmp.emplace_back(key, value);
      inserted = true;
    }
    tmp.emplace_back(key_array_[i], rid_array_[i]);
  }
  if (!inserted) {
    tmp.emplace_back(key, value);
  }
  int idx = 0;
  for (auto &p : tmp) {
    key_array_[idx] = p.first;
    rid_array_[idx] = p.second;
    idx++;
  }
  SetSize(idx);
  tombstone_cnt_ = 0;
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator,
                                        std::vector<ValueType> *result) const -> bool {
  auto pos = std::lower_bound(key_array_, key_array_ + GetSize(), key,
                              [&](const auto &a, const auto &b) { return comparator(a, b) < 0; });
  if (pos == key_array_ + GetSize()) {
    return false;
  }
  if (deleted_array_[pos - key_array_] != 0) {
    return false;
  }
  if (comparator(*pos, key) == 0) {
    result->push_back(rid_array_[pos - key_array_]);
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveFirstValid(std::pair<KeyType, ValueType> &out) {
  BUSTUB_ASSERT(IsAboveMinThreshold(), "Page below min threshold");
  int idx = 0;
  auto found = false;
  for (int i = 0; i < GetSize(); i++) {
    if (deleted_array_[i] != 0) {
      deleted_array_[i] = 0;
      continue;
    }
    if (!found) {
      found = true;
      out = {key_array_[i], rid_array_[i]};
      continue;
    }
    key_array_[idx] = key_array_[i];
    rid_array_[idx] = rid_array_[i];
    idx++;
  }
  SetSize(idx);
  tombstone_cnt_ = 0;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveLastValid(std::pair<KeyType, ValueType> &out) {
  BUSTUB_ASSERT(IsAboveMinThreshold(), "Page below min threshold");
  int idx = 0;
  for (int i = 0; i < GetSize(); i++) {
    if (deleted_array_[i] != 0) {
      deleted_array_[i] = 0;
      continue;
    }
    key_array_[idx] = key_array_[i];
    rid_array_[idx] = rid_array_[i];
    idx++;
  }
  BUSTUB_ASSERT(idx > 0, "Empty page");
  out = {key_array_[idx - 1], rid_array_[idx - 1]};
  SetSize(idx - 1);
  tombstone_cnt_ = 0;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator) -> bool {
  // lazy deletion
  auto pos = std::lower_bound(key_array_, key_array_ + GetSize(), key,
                              [&](const auto &a, const auto &b) { return comparator(a, b) < 0; });
  if (pos == key_array_ + GetSize()) {
    return false;
  }
  if (comparator(*pos, key) == 0) {
    if (deleted_array_[pos - key_array_] != 0) {
      return false;
    }
    deleted_array_[pos - key_array_] = 1;
    tombstone_cnt_++;
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::PushBack(std::vector<std::pair<KeyType, ValueType>> &items) {
  int idx = 0;
  for (int i = 0; i < GetSize(); i++) {
    if (deleted_array_[i] != 0) {
      deleted_array_[i] = 0;
      continue;
    }
    key_array_[idx] = key_array_[i];
    rid_array_[idx] = rid_array_[i];
    idx++;
  }
  for (size_t i = 0; i < items.size(); i++) {
    key_array_[idx] = items[i].first;
    rid_array_[idx] = items[i].second;
    idx++;
  }
  tombstone_cnt_ = 0;
  SetSize(idx);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Prepend(std::vector<std::pair<KeyType, ValueType>> &items) {
  std::vector<std::pair<KeyType, ValueType>> tmp;
  int idx = 0;
  for (int i = 0; i < GetSize(); i++) {
    if (deleted_array_[i] != 0) {
      deleted_array_[i] = 0;
      continue;
    }
    tmp.push_back({key_array_[i], rid_array_[i]});
  }
  for (size_t i = 0; i < items.size(); i++) {
    key_array_[idx] = items[i].first;
    rid_array_[idx] = items[i].second;
    idx++;
  }
  for (size_t i = 0; i < tmp.size(); i++) {
    key_array_[idx] = tmp[i].first;
    rid_array_[idx] = tmp[i].second;
    idx++;
  }
  SetSize(idx);
  tombstone_cnt_ = 0;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  auto pos = std::lower_bound(key_array_, key_array_ + GetSize(), key,
                              [&](const auto &a, const auto &b) { return comparator(a, b) < 0; });
  return pos - key_array_;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
