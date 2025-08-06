//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_leaf_page.h
//
// Identification: src/include/storage/page/b_plus_tree_leaf_page.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
#define LEAF_PAGE_HEADER_SIZE 64
#define LEAF_PAGE_SLOT_CNT \
  ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (sizeof(KeyType) + sizeof(ValueType) + sizeof(char)))

/**
 * Store indexed key and record id (record id = page id combined with slot id,
 * see `include/common/rid.h` for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 *  ---------
 * | HEADER |
 *  ---------
 *  ---------------------------------
 * | KEY(1) | KEY(2) | ... | KEY(n) |
 *  ---------------------------------
 *  ---------------------------------
 * | RID(1) | RID(2) | ... | RID(n) |
 *  ---------------------------------
 *
 *  Header format (size in byte, 16 bytes in total):
 *  -----------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |
 *  -----------------------------------------------
 *  -----------------
 * | NextPageId (4) |
 *  -----------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreeLeafPage() = delete;
  BPlusTreeLeafPage(const BPlusTreeLeafPage &other) = delete;

  void Init(int max_size = LEAF_PAGE_SLOT_CNT);

  // Helper methods
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);
  auto KeyAt(int index) const -> KeyType;
  auto ValueAt(int index) const -> ValueType;
  auto DeletedAt(int index) const -> bool;
  auto Key(int index) const -> const KeyType & { return key_array_[index]; }
  auto Value(int index) const -> const ValueType & { return rid_array_[index]; }

  auto GetPageId() const -> page_id_t;
  void SetPageId(page_id_t page_id);

  void SetKeyValueAt(int index, const KeyType &key, const ValueType &value);
  auto Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator,
              std::vector<std::pair<KeyType, ValueType>> &redistributions, bool is_optimistic) -> bool;
  auto Lookup(const KeyType &key, const KeyComparator &comparator, std::vector<ValueType> *result) const -> bool;
  auto Remove(const KeyType &key, const KeyComparator &comparator) -> bool;
  void RemoveFirstValid(std::pair<KeyType, ValueType> &out);
  void RemoveLastValid(std::pair<KeyType, ValueType> &out);
  void PushBack(std::vector<std::pair<KeyType, ValueType>> &items);
  void Prepend(std::vector<std::pair<KeyType, ValueType>> &items);
  auto IsDeleted(int index) const -> bool { return deleted_array_[index] != 0; }
  auto KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int;

  auto IsUnderflow() const -> bool { return GetSize() - tombstone_cnt_ < GetMinSize(); }
  auto IsAboveMinThreshold() const -> bool { return GetSize() - tombstone_cnt_ > GetMinSize(); }
  auto IsOverflow() const -> bool {
    if (GetMaxSize() < 8) {
      return GetSize() - tombstone_cnt_ >= GetMaxSize();
    }
    return GetSize() - tombstone_cnt_ >= GetMaxSize() * 15 / 16;
  }
  auto IsEmpty() const -> bool { return GetSize() == tombstone_cnt_; }
  auto IsFull() const -> bool { return GetSize() - tombstone_cnt_ == GetMaxSize(); }

  /**
   * @brief For test only return a string representing all keys in
   * this leaf page formatted as "(key1,key2,key3,...)"
   *
   * @return The string representation of all keys in the current internal page
   */
  auto ToString() const -> std::string {
    std::string kstr = "id: " + std::to_string(GetPageId()) + " (";
    bool first = true;

    for (int i = 0; i < GetSize(); i++) {
      KeyType key = KeyAt(i);
      if (first) {
        first = false;
      } else {
        kstr.append(",");
      }

      kstr.append(std::to_string(key.ToString()));
      if (deleted_array_[i]) {
        kstr.append("T");
      }
    }
    kstr.append(")");

    return kstr;
  }

  void IncVersion() { version_++; }
  auto GetVersion() const -> int { return version_; }

 private:
  page_id_t next_page_id_;
  page_id_t page_id_;
  int tombstone_cnt_{0};
  int version_{0};
  // Array members for page data.
  KeyType key_array_[LEAF_PAGE_SLOT_CNT];
  ValueType rid_array_[LEAF_PAGE_SLOT_CNT];
  char deleted_array_[LEAF_PAGE_SLOT_CNT];
  // (Fall 2024) Feel free to add more fields and helper functions below if needed
};

}  // namespace bustub
