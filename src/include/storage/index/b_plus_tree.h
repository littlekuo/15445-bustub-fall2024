//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.h
//
// Identification: src/include/storage/index/b_plus_tree.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * b_plus_tree.h
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
#pragma once

#include <algorithm>
#include <deque>
#include <filesystem>
#include <iostream>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

struct PrintableBPlusTree;

enum class OperationType { kRead, kInsert, kRemove };

/**
 * @brief Definition of the Context class.
 *
 * Hint: This class is designed to help you keep track of the pages
 * that you're modifying or accessing.
 */
class Context {
  bool is_valid_{true};

 public:
  // When you insert into / remove from the B+ tree, store the write guard of header page here.
  // Remember to drop the header page guard and set it to nullopt when you want to unlock all.
  std::optional<WritePageGuard> header_page_{std::nullopt};

  // Save the root page id here so that it's easier to know if the current page is the root page.
  page_id_t root_page_id_{INVALID_PAGE_ID};

  // Store the write guards of the pages that you're modifying here.
  std::deque<WritePageGuard> write_set_;

  // You may want to use this when getting value, but not necessary.
  std::deque<ReadPageGuard> read_set_;

  auto IsRootPage(page_id_t page_id) -> bool { return page_id == root_page_id_; }

  auto DropHeaderAndWrites() -> void {
    if (header_page_.has_value()) {
      header_page_.value().Drop();
      header_page_ = std::nullopt;
    }
    for (auto &write_guard : write_set_) {
      write_guard.Drop();
    }
    write_set_.clear();
  }

  auto Drop() -> void {
    if (!is_valid_) {
      return;
    }
    if (header_page_.has_value()) {
      header_page_.value().Drop();
      header_page_ = std::nullopt;
    }
    for (auto &write_guard : write_set_) {
      write_guard.Drop();
    }
    write_set_.clear();
    for (auto &read_guard : read_set_) {
      read_guard.Drop();
    }
    read_set_.clear();
    root_page_id_ = INVALID_PAGE_ID;
    is_valid_ = false;
  }
  ~Context() { Drop(); }
};

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

// Main class providing the API for the Interactive B+ Tree.
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                     const KeyComparator &comparator, int leaf_max_size = LEAF_PAGE_SLOT_CNT,
                     int internal_max_size = INTERNAL_PAGE_SLOT_CNT);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key);

  // Return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool;

  // Return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // Index iterator
  auto Begin() -> INDEXITERATOR_TYPE;

  auto End() -> INDEXITERATOR_TYPE;

  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;

  void Print(BufferPoolManager *bpm);

  void Draw(BufferPoolManager *bpm, const std::filesystem::path &outf);

  auto DrawBPlusTree() -> std::string;

  // read data from file and insert one by one
  void InsertFromFile(const std::filesystem::path &file_name);

  // read data from file and remove one by one
  void RemoveFromFile(const std::filesystem::path &file_name);

  void BatchOpsFromFile(const std::filesystem::path &file_name);

 private:
  void ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out);

  void PrintTree(page_id_t page_id, const BPlusTreePage *page);

  auto ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree;

  auto FindLeftmostLeafPage() -> std::optional<ReadPageGuard> {
    auto cur_guard = bpm_->ReadPage(header_page_id_);
    page_id_t cur_page_id = cur_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
    if (cur_page_id == INVALID_PAGE_ID) {
      return std::nullopt;
    }
    while (true) {
      cur_guard = bpm_->ReadPage(cur_page_id);
      auto page = cur_guard.As<BPlusTreePage>();
      if (page->IsLeafPage()) {
        break;
      }
      auto internal_page = cur_guard.As<InternalPage>();
      cur_page_id = internal_page->ValueAt(0);
    }
    return cur_guard;
  }

  auto FindLeafPage(const KeyType &key, Context *ctx, OperationType op)
      -> std::variant<std::monostate, WritePageGuard, ReadPageGuard> {
    if (op == OperationType::kRead) {
      auto guard = bpm_->ReadPage(header_page_id_);
      auto header_page = guard.As<BPlusTreeHeaderPage>();
      if (header_page->root_page_id_ == INVALID_PAGE_ID) {
        return std::monostate{};
      }
      ctx->root_page_id_ = header_page->root_page_id_;
      guard = bpm_->ReadPage(ctx->root_page_id_);
      auto page = guard.As<BPlusTreePage>();
      while (!page->IsLeafPage()) {
        auto internal_page = guard.As<InternalPage>();
        auto idx = internal_page->PageIndexByKey(key, comparator_);
        auto child_page_id = internal_page->ValueAt(idx);
        guard = bpm_->ReadPage(child_page_id);
        page = guard.As<BPlusTreePage>();
      }
      return guard;
    }

    WritePageGuard guard = bpm_->WritePage(header_page_id_);
    ctx->header_page_ = std::move(guard);
    auto header_page = ctx->header_page_->AsMut<BPlusTreeHeaderPage>();
    ctx->root_page_id_ = header_page->root_page_id_;
    if (header_page->root_page_id_ == INVALID_PAGE_ID) {
      if (op == OperationType::kRemove) {
        ctx->Drop();
        return std::monostate{};
      }
      auto new_root_page_id = bpm_->NewPage();
      WritePageGuard new_guard = bpm_->WritePage(new_root_page_id);
      header_page->root_page_id_ = new_root_page_id;
      ctx->root_page_id_ = new_root_page_id;
      ctx->Drop();
      auto new_page = new_guard.AsMut<LeafPage>();
      new_page->Init(leaf_max_size_);
      new_page->SetPageId(new_root_page_id);
      return new_guard;
    }
    WritePageGuard cur_guard = bpm_->WritePage(ctx->root_page_id_);
    auto page_full = cur_guard.AsMut<BPlusTreePage>()->IsFull();
    auto page_above_min = cur_guard.AsMut<BPlusTreePage>()->IsAboveMinThreshold();
    if ((op == OperationType::kInsert && !page_full) || (op == OperationType::kRemove && page_above_min)) {
      ctx->DropHeaderAndWrites();
    }
    auto page = cur_guard.AsMut<BPlusTreePage>();
    while (!page->IsLeafPage()) {
      auto internal_page = cur_guard.AsMut<InternalPage>();
      auto idx = internal_page->PageIndexByKey(key, comparator_);
      auto child_page_id = internal_page->ValueAt(idx);
      ctx->write_set_.push_back(std::move(cur_guard));
      cur_guard = bpm_->WritePage(child_page_id);
      page_full = cur_guard.AsMut<BPlusTreePage>()->IsFull();
      page_above_min = cur_guard.AsMut<BPlusTreePage>()->IsAboveMinThreshold();
      if ((op == OperationType::kInsert && !page_full) || (op == OperationType::kRemove && page_above_min)) {
        ctx->DropHeaderAndWrites();
      }
      page = cur_guard.AsMut<BPlusTreePage>();
    }
    return cur_guard;
  }

  auto SplitLeafPage(WritePageGuard &old_page, Context *ctx,
                     std::vector<std::pair<KeyType, ValueType>> &redistributions) -> page_id_t;

  auto SplitInternalPage(WritePageGuard &old_page, Context *ctx,
                         std::vector<std::pair<KeyType, page_id_t>> &redistributions) -> page_id_t;

  auto MergeOrRedistributeLeafPage(WritePageGuard *page_guard, Context *ctx) -> page_id_t;

  auto MergeOrRedistributeInternalPage(WritePageGuard *page_guard, Context *ctx) -> page_id_t;

  // member variable
  std::string index_name_;
  BufferPoolManager *bpm_;
  KeyComparator comparator_;
  std::vector<std::string> log;  // NOLINT
  int leaf_max_size_;
  int internal_max_size_;
  page_id_t header_page_id_;
};

/**
 * @brief for test only. PrintableBPlusTree is a printable B+ tree.
 * We first convert B+ tree into a printable B+ tree and the print it.
 */
struct PrintableBPlusTree {
  int size_;
  std::string keys_;
  std::vector<PrintableBPlusTree> children_;

  /**
   * @brief BFS traverse a printable B+ tree and print it into
   * into out_buf
   *
   * @param out_buf
   */
  void Print(std::ostream &out_buf) {
    std::vector<PrintableBPlusTree *> que = {this};
    while (!que.empty()) {
      std::vector<PrintableBPlusTree *> new_que;

      for (auto &t : que) {
        int padding = (t->size_ - t->keys_.size()) / 2;
        out_buf << std::string(padding, ' ');
        out_buf << t->keys_;
        out_buf << std::string(padding, ' ');

        for (auto &c : t->children_) {
          new_que.push_back(&c);
        }
      }
      out_buf << "\n";
      que = new_que;
    }
  }
};

}  // namespace bustub
