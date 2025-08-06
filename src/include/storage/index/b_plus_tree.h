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
#include <atomic>
#include <deque>
#include <filesystem>
#include <iostream>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>
#include <unordered_map>
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

enum class OperationType { kInsert, kRemove, kHandleUnderflow };

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
  // std::optional<WritePageGuard> header_page_{std::nullopt};

  std::optional<std::lock_guard<std::shared_mutex>> header_page_lock_;
  // Save the root page id here so that it's easier to know if the current page is the root page.
  page_id_t root_page_id_{INVALID_PAGE_ID};

  // Store the write guards of the pages that you're modifying here.
  std::deque<WritePageGuard> write_set_;

  // You may want to use this when getting value, but not necessary.
  std::deque<ReadPageGuard> read_set_;

  auto IsRootPage(page_id_t page_id) -> bool { return page_id == root_page_id_; }

  auto DropHeaderAndWrites() -> void {
    if (header_page_lock_.has_value()) {
      header_page_lock_.reset();
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
    if (header_page_lock_.has_value()) {
      header_page_lock_.reset();
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

template <class KeyType, class ValueType>
class ProcessingSet {
 public:
  ProcessingSet() = default;
  ~ProcessingSet() = default;

  void Put(const std::pair<KeyType, ValueType> &element) {
    std::unique_lock<std::mutex> lk(m_);
    q_[element.first] = element.second;
  }

  auto Get() -> std::optional<std::pair<KeyType, ValueType>> {
    std::unique_lock<std::mutex> lk(m_);
    if (q_.empty()) {
      return std::nullopt;
    }
    std::pair<KeyType, ValueType> element{q_.begin()->first, q_.begin()->second};
    q_.erase(q_.begin());
    return element;
  }

  auto GetAll(std::vector<std::pair<KeyType, ValueType>> &result) -> void {
    std::unique_lock<std::mutex> lk(m_);
    if (q_.empty()) {
      return;
    }
    for (auto it = q_.begin(); it != q_.end(); ++it) {
      result.emplace_back(it->first, it->second);
    }
    q_.clear();
  }

 private:
  std::mutex m_;
  std::unordered_map<KeyType, ValueType> q_;
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

  ~BPlusTree();

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
    header_mtx_.lock_shared();
    if (root_page_id_ == INVALID_PAGE_ID) {
      header_mtx_.unlock_shared();
      return std::nullopt;
    }
    ReadPageGuard cur_guard = bpm_->ReadPage(root_page_id_);
    header_mtx_.unlock_shared();
    while (true) {
      auto page = cur_guard.As<BPlusTreePage>();
      if (page->IsLeafPage()) {
        break;
      }
      auto internal_page = cur_guard.As<InternalPage>();
      auto cur_page_id = internal_page->ValueAt(0);
      cur_guard = bpm_->ReadPage(cur_page_id);
    }
    return cur_guard;
  }

  auto FindLeafPageOptimistic(const KeyType &key, Context *ctx) -> std::optional<ReadPageGuard> {
    header_mtx_.lock_shared();
    if (root_page_id_ == INVALID_PAGE_ID) {
      header_mtx_.unlock_shared();
      return std::nullopt;
    }
    ReadPageGuard guard = bpm_->ReadPage(root_page_id_);
    header_mtx_.unlock_shared();
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

  auto FindLeafPagePessimistic(const KeyType &key, Context *ctx, OperationType op) -> std::optional<WritePageGuard> {
    if (op == OperationType::kRemove) {
      header_mtx_.lock_shared();
      if (root_page_id_ == INVALID_PAGE_ID) {
        header_mtx_.unlock_shared();
        return std::nullopt;
      }
      WritePageGuard guard = bpm_->WritePage(root_page_id_);
      header_mtx_.unlock_shared();
      auto page = guard.As<BPlusTreePage>();
      while (!page->IsLeafPage()) {
        auto internal_page = guard.As<InternalPage>();
        auto idx = internal_page->PageIndexByKey(key, comparator_);
        auto child_page_id = internal_page->ValueAt(idx);
        guard = bpm_->WritePage(child_page_id);
        page = guard.As<BPlusTreePage>();
      }
      return guard;
    }

    // consider overflow or underflow
    ctx->header_page_lock_.emplace(header_mtx_);
    ctx->root_page_id_ = root_page_id_;
    if (root_page_id_ == INVALID_PAGE_ID) {
      if (op == OperationType::kHandleUnderflow) {
        ctx->Drop();
        return std::nullopt;
      }
      auto new_root_page_id = bpm_->NewPage();
      WritePageGuard new_guard = bpm_->WritePage(new_root_page_id);
      WritePageGuard header_guard = bpm_->WritePage(header_page_id_);
      header_guard.AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_root_page_id;
      root_page_id_ = new_root_page_id;
      ctx->root_page_id_ = new_root_page_id;
      leaf_cnt_.fetch_add(1, std::memory_order_relaxed);
      auto new_page = new_guard.AsMut<LeafPage>();
      new_page->Init(leaf_max_size_);
      new_page->SetPageId(new_root_page_id);
      return new_guard;
    }
    WritePageGuard cur_guard = bpm_->WritePage(ctx->root_page_id_);
    auto page = cur_guard.AsMut<BPlusTreePage>();
    auto page_overflow = IsOverflow(cur_guard.GetDataMut(), page->IsLeafPage());
    auto page_above_min = IsAboveMinThreshold(cur_guard.GetDataMut(), page->IsLeafPage());
    if ((op == OperationType::kInsert && !page_overflow) || (op == OperationType::kHandleUnderflow && page_above_min)) {
      ctx->DropHeaderAndWrites();
    }
    while (!page->IsLeafPage()) {
      auto internal_page = cur_guard.AsMut<InternalPage>();
      auto idx = internal_page->PageIndexByKey(key, comparator_);
      auto child_page_id = internal_page->ValueAt(idx);
      ctx->write_set_.push_back(std::move(cur_guard));
      cur_guard = bpm_->WritePage(child_page_id);
      page = cur_guard.AsMut<BPlusTreePage>();
      page_overflow = IsOverflow(cur_guard.GetDataMut(), page->IsLeafPage());
      page_above_min = IsAboveMinThreshold(cur_guard.GetDataMut(), page->IsLeafPage());
      if ((op == OperationType::kInsert && !page_overflow) ||
          (op == OperationType::kHandleUnderflow && page_above_min)) {
        ctx->DropHeaderAndWrites();
      }
    }
    return cur_guard;
  }

  auto FindLeafPageOptimisticInsert(const KeyType &key, Context *ctx)
      -> std::variant<std::monostate, WritePageGuard, ReadPageGuard> {
    header_mtx_.lock_shared();
    if (root_page_id_ == INVALID_PAGE_ID) {
      header_mtx_.unlock_shared();
      return std::monostate{};
    }
    ReadPageGuard guard = bpm_->ReadPage(root_page_id_);
    header_mtx_.unlock_shared();
    auto page = guard.As<BPlusTreePage>();
    if (page->IsLeafPage()) {
      return guard;
    }
    while (true) {
      auto internal_page = guard.As<InternalPage>();
      auto page_id = internal_page->GetPageId();
      auto version = internal_page->GetVersion();
      auto idx = internal_page->PageIndexByKey(key, comparator_);
      auto child_page_id = internal_page->ValueAt(idx);
      ReadPageGuard child_guard = bpm_->ReadPage(child_page_id);
      auto child_page = child_guard.As<BPlusTreePage>();
      if (!child_page->IsLeafPage()) {
        guard = std::move(child_guard);
      } else if (!child_guard.As<LeafPage>()->IsFull()) {
        guard = std::move(child_guard);
        break;
      } else if (internal_page->PessimisticFirst()) {
        return std::monostate{};
      } else {
        // std::cout << "optimistic split" << std::endl;
        // internal page is not full and leaf page is full
        child_guard.Drop();
        guard.Drop();
        WritePageGuard cur_guard = bpm_->WritePage(page_id);
        internal_page = cur_guard.AsMut<InternalPage>();
        if (internal_page->IsFull() || internal_page->GetVersion() != version) {
          // version changed or full, retry by pessimistic
          cur_guard.Drop();
          return std::monostate{};
        }
        idx = internal_page->PageIndexByKey(key, comparator_);
        child_page_id = internal_page->ValueAt(idx);
        WritePageGuard child_write_guard = bpm_->WritePage(child_page_id);
        if (!child_write_guard.As<LeafPage>()->IsFull()) {
          cur_guard.Drop();
          return child_write_guard;
        }
        ctx->write_set_.push_back(std::move(cur_guard));
        return child_write_guard;
      }
    }
    return guard;
  }

  auto IsAboveMinThreshold(const char *data, bool is_leaf) -> bool {
    if (is_leaf) {
      return reinterpret_cast<const LeafPage *>(data)->IsAboveMinThreshold();
    }
    return reinterpret_cast<const InternalPage *>(data)->IsAboveMinThreshold();
  }

  auto IsUnderflow(const char *data, bool is_leaf) -> bool {
    if (is_leaf) {
      return reinterpret_cast<const LeafPage *>(data)->IsUnderflow();
    }
    return reinterpret_cast<const InternalPage *>(data)->IsUnderflow();
  }

  auto IsOverflow(const char *data, bool is_leaf) -> bool {
    if (is_leaf) {
      return reinterpret_cast<const LeafPage *>(data)->IsOverflow();
    }
    return reinterpret_cast<const InternalPage *>(data)->IsOverflow();
  }

  void AddUnderflowPage(page_id_t page_id, KeyType key) { underflow_pages_.Put(std::make_pair(page_id, key)); }

  auto SplitLeafPage(WritePageGuard &old_page, Context *ctx,
                     std::vector<std::pair<KeyType, ValueType>> &redistributions) -> page_id_t;

  auto SplitInternalPage(WritePageGuard &old_page, Context *ctx,
                         std::vector<std::pair<KeyType, page_id_t>> &redistributions) -> page_id_t;

  auto MergeOrRedistributeLeafPage(WritePageGuard *page_guard, Context *ctx) -> page_id_t;

  auto MergeOrRedistributeInternalPage(WritePageGuard *page_guard, Context *ctx) -> page_id_t;

  void HandleUnderflowPage(page_id_t target_page_id, KeyType &key);

  // member variable
  alignas(64) std::atomic<size_t> logical_size_{0};
  std::thread worker_thread_;
  std::chrono::milliseconds interval_{5000};
  BufferPoolManager *bpm_;
  KeyComparator comparator_;
  std::vector<std::string> log;  // NOLINT
  alignas(64) std::atomic<size_t> active_pessimistic_cnt_{0};
  std::string index_name_;
  std::condition_variable cv_;
  ProcessingSet<page_id_t, KeyType> underflow_pages_;
  page_id_t root_page_id_;
  int leaf_max_size_;
  alignas(64) std::atomic<size_t> pessimistic_cnt_{0};
  int internal_max_size_;
  page_id_t header_page_id_;
  std::atomic<bool> stop_flag_{false};
  alignas(64) std::atomic<size_t> leaf_cnt_{0};
  alignas(64) std::shared_mutex header_mtx_;  // protect header_page_(root_page_id_)

  alignas(64) std::mutex mtx_;
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
