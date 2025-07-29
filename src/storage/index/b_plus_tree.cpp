//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"
#include <atomic>
#include "common/config.h"
#include "storage/index/b_plus_tree_debug.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      index_name_(std::move(name)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  auto header_guard = bpm_->WritePage(header_page_id);
  auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = INVALID_PAGE_ID;
  root_page_id_ = header_page->root_page_id_;
  std::cout << "concurrency: " << std::thread::hardware_concurrency() << " leaf_size: " << LEAF_PAGE_SLOT_CNT
            << std::endl;
  worker_thread_ = std::thread([this]() {
    auto next_run_time = std::chrono::steady_clock::now() + interval_;

    while (!stop_flag_.load()) {
      std::unique_lock<std::mutex> lock(mtx_);
      if (cv_.wait_until(lock, next_run_time, [this] { return stop_flag_.load(); })) {
        break;
      }
      lock.unlock();

      std::vector<std::pair<page_id_t, KeyType>> tasks_to_process;
      underflow_pages_.GetAll(tasks_to_process);
      if (!tasks_to_process.empty()) {
        std::cout << "[Worker] Processing " << tasks_to_process.size() << " tasks." << std::endl;
        for (auto &task : tasks_to_process) {
          // std::cout << "[Worker] Processing task: page_id=" << task.first << ", key=" << task.second << std::endl;
          HandleUnderflowPage(task.first, task.second);
        }
      }

      next_run_time += interval_;
    }
    // std::cout << "[Worker] Thread stopped." << std::endl;
  });
}

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::~BPLUSTREE_TYPE() {
  std::cout << "pessimistic count: " << pessimistic_cnt_.load(std::memory_order_relaxed)
            << " active count: " << active_pessimistic_cnt_.load(std::memory_order_relaxed)
            << " leaf count: " << leaf_cnt_.load(std::memory_order_relaxed) << std::endl;
  stop_flag_.store(true);
  cv_.notify_one();
  worker_thread_.join();
}

/**
 * @brief Helper function to decide whether current b+tree is empty
 * @return Returns true if this B+ tree has no keys and values.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return logical_size_.load(std::memory_order_relaxed) == 0; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
 * @brief Return the only value that associated with input key
 *
 * This method is used for point query
 *
 * @param key input key
 * @param[out] result vector that stores the only value that associated with input key, if the value exists
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  // Declaration of context instance.
  Context ctx;
  auto guard_opt = FindLeafPageOptimistic(key, &ctx);
  if (!guard_opt.has_value()) {
    return false;
  }
  ReadPageGuard guard = std::move(guard_opt.value());
  auto leaf_page = guard.As<LeafPage>();
  return leaf_page->Lookup(key, comparator_, result);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * @brief Insert constant key & value pair into b+ tree
 *
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 *
 * @param key the key to insert
 * @param value the value associated with key
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  // Declaration of context instance.
  Context ctx;
  WritePageGuard cur_guard;
  uint64_t average_cnt = 0;
  if (leaf_cnt_.load(std::memory_order_relaxed) != 0) {
    average_cnt = logical_size_.load(std::memory_order_relaxed) / leaf_cnt_.load(std::memory_order_relaxed);
  }
  auto is_optimistic = false;
  if (leaf_max_size_ < 3 || average_cnt > static_cast<uint64_t>(leaf_max_size_) * 0.7) {
    active_pessimistic_cnt_.fetch_add(1, std::memory_order_relaxed);
    cur_guard = FindLeafPagePessimistic(key, &ctx, OperationType::kInsert).value();
  } else {
    auto guard_variant = FindLeafPageOptimisticInsert(key, &ctx);
    // try optimistic first, if failed, then try pessimistic
    if (std::holds_alternative<std::monostate>(guard_variant)) {
      // pessimistic
      pessimistic_cnt_.fetch_add(1, std::memory_order_relaxed);
      cur_guard = FindLeafPagePessimistic(key, &ctx, OperationType::kInsert).value();
    } else if (std::holds_alternative<ReadPageGuard>(guard_variant)) {
      ReadPageGuard read_guard = std::move(std::get<ReadPageGuard>(guard_variant));
      auto cur_version = read_guard.As<LeafPage>()->GetVersion();
      auto cur_page_id = read_guard.As<LeafPage>()->GetPageId();
      read_guard.Drop();
      cur_guard = bpm_->WritePage(cur_page_id);
      auto leaf_page = cur_guard.AsMut<LeafPage>();
      if (leaf_page->GetVersion() != cur_version || leaf_page->IsFull()) {
        cur_guard.Drop();
        // pessimistic
        pessimistic_cnt_.fetch_add(1, std::memory_order_relaxed);
        cur_guard = FindLeafPagePessimistic(key, &ctx, OperationType::kInsert).value();
      } else {
        is_optimistic = true;
      }
    } else {
      is_optimistic = true;
      cur_guard = std::move(std::get<WritePageGuard>(guard_variant));
    }
  }

  auto leaf_page = cur_guard.AsMut<LeafPage>();
  auto cur_page_id = leaf_page->GetPageId();
  std::vector<std::pair<KeyType, ValueType>> redistributions;
  auto inserted = leaf_page->Insert(key, value, comparator_, redistributions, is_optimistic);
  if (!inserted) {
    ctx.Drop();
    return false;
  }
  if (redistributions.empty()) {
    ctx.Drop();
    logical_size_.fetch_add(1, std::memory_order_relaxed);
    return true;
  }
  auto new_leaf_page_id = SplitLeafPage(cur_guard, &ctx, redistributions);
  cur_guard.Drop();
  std::pair<KeyType, page_id_t> separator{redistributions[0].first, new_leaf_page_id};
  bool need_promote = true;
  // 2.3 insert into parent page
  while (need_promote) {
    // 2.3.1 handle root page
    if (ctx.IsRootPage(cur_page_id)) {
      need_promote = false;
      auto new_root_page_id = bpm_->NewPage();
      WritePageGuard new_root_guard = bpm_->WritePage(new_root_page_id);
      WritePageGuard header_guard = bpm_->WritePage(header_page_id_);
      header_guard.AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_root_page_id;
      root_page_id_ = new_root_page_id;
      ctx.Drop();
      auto new_root_page = new_root_guard.AsMut<InternalPage>();
      new_root_page->Init(internal_max_size_);
      new_root_page->SetPageId(new_root_page_id);
      new_root_page->ChangeSizeBy(2);
      new_root_page->SetKeyAt(1, separator.first);
      new_root_page->SetValueAt(0, cur_page_id);
      new_root_page->SetValueAt(1, separator.second);
      break;
    }
    // 2.3.2 handle parent page
    auto cur_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    BUSTUB_ASSERT(!cur_guard.AsMut<BPlusTreePage>()->IsLeafPage(), "parent page should not be leaf page");
    auto cur_page = cur_guard.AsMut<InternalPage>();
    std::vector<std::pair<KeyType, page_id_t>> redistributions;
    auto inserted = cur_page->Insert(separator.first, separator.second, comparator_, redistributions);
    BUSTUB_ASSERT(inserted, "insert should succeed");
    if (redistributions.empty()) {
      ctx.Drop();
      break;
    }
    auto new_page_id = SplitInternalPage(cur_guard, &ctx, redistributions);
    separator = {redistributions[0].first, new_page_id};
    cur_page_id = cur_page->GetPageId();
    cur_guard.Drop();
  }
  logical_size_.fetch_add(1, std::memory_order_relaxed);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * @brief Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 *
 * @param key input key
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Declaration of context instance.
  Context ctx;
  // try optimistic remove first, if failed, do pessimistic remove
  auto guard_opt = FindLeafPageOptimistic(key, &ctx);
  if (!guard_opt.has_value()) {
    return;
  }
  ReadPageGuard read_guard = std::move(guard_opt.value());
  auto cur_version = read_guard.As<LeafPage>()->GetVersion();
  auto cur_page_id = read_guard.As<LeafPage>()->GetPageId();
  read_guard.Drop();
  WritePageGuard cur_guard = bpm_->WritePage(cur_page_id);
  if (cur_guard.AsMut<LeafPage>()->GetVersion() != cur_version) {
    cur_guard.Drop();
    pessimistic_cnt_.fetch_add(1, std::memory_order_relaxed);
    auto guard_opt = FindLeafPagePessimistic(key, &ctx, OperationType::kRemove);
    if (!guard_opt.has_value()) {
      return;
    }
    cur_guard = std::move(guard_opt.value());
  }

  auto leaf_page = cur_guard.AsMut<LeafPage>();
  auto removed = leaf_page->Remove(key, comparator_);
  if (!removed) {
    ctx.Drop();
    return;
  }
  logical_size_.fetch_sub(1, std::memory_order_relaxed);
  cur_page_id = leaf_page->GetPageId();
  if (!leaf_page->IsUnderflow()) {
    ctx.Drop();
    return;
  }
  cur_guard.Drop();
  ctx.Drop();
  AddUnderflowPage(cur_page_id, key);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/**
 * @brief Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  auto guard = FindLeftmostLeafPage();
  if (!guard.has_value()) {
    return End();
  }
  auto it = INDEXITERATOR_TYPE(bpm_, 0, std::move(guard.value()));
  it.Advance();
  return it;
}

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  Context ctx;
  auto guard_variant = FindLeafPageOptimistic(key, &ctx);
  if (!guard_variant.has_value()) {
    return End();
  }
  ReadPageGuard guard = std::move(guard_variant.value());
  auto leaf_page = guard.As<LeafPage>();
  int index = leaf_page->KeyIndex(key, comparator_);
  auto it = INDEXITERATOR_TYPE(bpm_, index, std::move(guard));
  it.Advance();
  return it;
}

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  if (IsEmpty()) {
    return INVALID_PAGE_ID;
  }
  header_mtx_.lock_shared();
  auto root_page_id = root_page_id_;
  header_mtx_.unlock_shared();
  return root_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeafPage(WritePageGuard &old_page, Context *ctx,
                                   std::vector<std::pair<KeyType, ValueType>> &redistributions) -> page_id_t {
  BUSTUB_ASSERT(old_page.AsMut<BPlusTreePage>()->IsLeafPage(), "page should be leaf page");

  auto leaf_page = old_page.AsMut<LeafPage>();
  auto new_leaf_page_id = bpm_->NewPage();
  WritePageGuard new_leaf_guard = bpm_->WritePage(new_leaf_page_id);
  auto new_leaf_page = new_leaf_guard.AsMut<LeafPage>();
  new_leaf_page->Init(leaf_max_size_);
  new_leaf_page->SetPageId(new_leaf_page_id);
  new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_leaf_page_id);
  new_leaf_page->SetSize(redistributions.size());
  leaf_page->IncVersion();
  leaf_cnt_.fetch_add(1, std::memory_order_relaxed);
  for (size_t idx = 0; idx < redistributions.size(); idx++) {
    new_leaf_page->SetKeyValueAt(idx, redistributions[idx].first, redistributions[idx].second);
  }
  return new_leaf_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternalPage(WritePageGuard &old_page, Context *ctx,
                                       std::vector<std::pair<KeyType, page_id_t>> &redistributions) -> page_id_t {
  BUSTUB_ASSERT(!old_page.AsMut<BPlusTreePage>()->IsLeafPage(), "page should not be leaf page");

  old_page.AsMut<InternalPage>()->IncVersion();
  auto new_page_id = bpm_->NewPage();
  WritePageGuard new_guard = bpm_->WritePage(new_page_id);
  auto new_page = new_guard.AsMut<InternalPage>();
  new_page->Init(internal_max_size_);
  new_page->SetPageId(new_page_id);
  new_page->SetSize(redistributions.size());
  for (size_t idx = 0; idx < redistributions.size(); idx++) {
    if (idx != 0) {
      new_page->SetKeyAt(idx, redistributions[idx].first);
    }
    new_page->SetValueAt(idx, redistributions[idx].second);
  }
  return new_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MergeOrRedistributeLeafPage(WritePageGuard *page_guard, Context *ctx) -> page_id_t {
  auto leaf_page = page_guard->AsMut<LeafPage>();
  BUSTUB_ASSERT(leaf_page->IsUnderflow(), "page should be underflow");
  WritePageGuard parent_guard = std::move(ctx->write_set_.back());
  ctx->write_set_.pop_back();
  auto parent_page = parent_guard.AsMut<InternalPage>();
  auto idx = parent_page->ValueIndex(page_guard->GetPageId());
  if (idx < parent_page->GetSize() - 1) {
    WritePageGuard right_sibling_guard = bpm_->WritePage(parent_page->ValueAt(idx + 1));
    auto right_sibling_page = right_sibling_guard.AsMut<LeafPage>();
    if (right_sibling_page->IsAboveMinThreshold()) {
      std::pair<KeyType, ValueType> item;
      right_sibling_page->RemoveFirstValid(item);
      std::vector<std::pair<KeyType, ValueType>> items;
      items.push_back(item);
      leaf_page->PushBack(items);
      leaf_page->IncVersion();
      right_sibling_page->IncVersion();
      parent_page->SetKeyAt(idx + 1, right_sibling_page->KeyAt(0));
      right_sibling_guard.Drop();
      ctx->Drop();
      return leaf_page->GetPageId();
    }
    leaf_cnt_.fetch_sub(1, std::memory_order_relaxed);
    std::vector<std::pair<KeyType, ValueType>> items;
    for (int i = 0; i < right_sibling_page->GetSize(); i++) {
      if (!right_sibling_page->IsDeleted(i)) {
        items.push_back({right_sibling_page->KeyAt(i), right_sibling_page->ValueAt(i)});
      }
    }
    leaf_page->IncVersion();
    right_sibling_page->IncVersion();
    leaf_page->PushBack(items);
    leaf_page->SetNextPageId(right_sibling_page->GetNextPageId());
    parent_page->RemoveAt(idx + 1);
    ctx->write_set_.push_back(std::move(parent_guard));
    right_sibling_guard.Drop();
    return leaf_page->GetPageId();
  }
  auto cur_page_id = page_guard->GetPageId();
  // ensure the lock order
  page_guard->Drop();
  WritePageGuard left_sibling_guard = bpm_->WritePage(parent_page->ValueAt(idx - 1));
  *page_guard = bpm_->WritePage(cur_page_id);
  leaf_page = page_guard->AsMut<LeafPage>();
  auto left_sibling_page = left_sibling_guard.AsMut<LeafPage>();
  if (left_sibling_page->IsAboveMinThreshold()) {
    std::pair<KeyType, ValueType> item;
    left_sibling_page->RemoveLastValid(item);
    std::vector<std::pair<KeyType, ValueType>> items;
    items.push_back(item);
    leaf_page->Prepend(items);
    parent_page->SetKeyAt(idx, item.first);
    leaf_page->IncVersion();
    left_sibling_page->IncVersion();
    left_sibling_guard.Drop();
    ctx->Drop();
    return leaf_page->GetPageId();
  }
  std::vector<std::pair<KeyType, ValueType>> items;
  for (int i = 0; i < leaf_page->GetSize(); i++) {
    if (!leaf_page->IsDeleted(i)) {
      items.push_back({leaf_page->KeyAt(i), leaf_page->ValueAt(i)});
    }
  }
  leaf_cnt_.fetch_sub(1, std::memory_order_relaxed);
  left_sibling_page->PushBack(items);
  parent_page->RemoveAt(idx);
  left_sibling_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->IncVersion();
  left_sibling_page->IncVersion();
  left_sibling_guard.Drop();
  ctx->write_set_.push_back(std::move(parent_guard));
  return left_sibling_page->GetPageId();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MergeOrRedistributeInternalPage(WritePageGuard *page_guard, Context *ctx) -> page_id_t {
  auto internal_page = page_guard->AsMut<InternalPage>();
  BUSTUB_ASSERT(internal_page->IsUnderflow(), "page should be underflow");
  WritePageGuard parent_guard = std::move(ctx->write_set_.back());
  ctx->write_set_.pop_back();
  auto parent_page = parent_guard.AsMut<InternalPage>();
  auto idx = parent_page->ValueIndex(page_guard->GetPageId());
  // 1. right sibling
  if (idx < parent_page->GetSize() - 1) {
    WritePageGuard right_sibling_guard = bpm_->WritePage(parent_page->ValueAt(idx + 1));
    auto right_sibling_page = right_sibling_guard.AsMut<InternalPage>();
    if (right_sibling_page->IsAboveMinThreshold()) {
      auto key = right_sibling_page->KeyAt(1);
      auto value = right_sibling_page->ValueAt(0);
      right_sibling_page->RemoveAt(0);
      internal_page->ChangeSizeBy(1);
      internal_page->SetKeyAt(internal_page->GetSize() - 1, parent_page->KeyAt(idx + 1));
      internal_page->SetValueAt(internal_page->GetSize() - 1, value);
      internal_page->IncVersion();
      right_sibling_page->IncVersion();
      parent_page->SetKeyAt(idx + 1, key);
      right_sibling_guard.Drop();
      ctx->Drop();
      return internal_page->GetPageId();
    }
    auto old_size = internal_page->GetSize();
    internal_page->ChangeSizeBy(right_sibling_page->GetSize());
    internal_page->SetKeyAt(old_size, parent_page->KeyAt(idx + 1));
    for (int i = 0; i < right_sibling_page->GetSize(); i++) {
      if (i != 0) {
        internal_page->SetKeyAt(old_size + i, right_sibling_page->KeyAt(i));
      }
      internal_page->SetValueAt(old_size + i, right_sibling_page->ValueAt(i));
    }
    internal_page->IncVersion();
    right_sibling_page->IncVersion();
    parent_page->RemoveAt(idx + 1);
    ctx->write_set_.push_back(std::move(parent_guard));
    right_sibling_guard.Drop();
    return internal_page->GetPageId();
  }
  // 2. left sibling
  auto cur_page_id = page_guard->GetPageId();
  // ensure the lock order
  page_guard->Drop();
  WritePageGuard left_sibling_guard = bpm_->WritePage(parent_page->ValueAt(idx - 1));
  *page_guard = bpm_->WritePage(cur_page_id);
  internal_page = page_guard->AsMut<InternalPage>();
  auto left_sibling_page = left_sibling_guard.AsMut<InternalPage>();
  if (left_sibling_page->IsAboveMinThreshold()) {
    auto key = parent_page->KeyAt(idx);
    auto old_size = internal_page->GetSize();
    internal_page->ChangeSizeBy(1);
    for (int i = 0; i < old_size; i++) {
      if (i != 0) {
        internal_page->SetKeyAt(i + 1, internal_page->KeyAt(i));
      }
      internal_page->SetValueAt(i + 1, internal_page->ValueAt(i));
    }
    internal_page->SetKeyAt(1, key);
    internal_page->SetValueAt(0, left_sibling_page->ValueAt(left_sibling_page->GetSize() - 1));
    internal_page->IncVersion();
    left_sibling_page->IncVersion();
    parent_page->SetKeyAt(idx, left_sibling_page->KeyAt(left_sibling_page->GetSize() - 1));
    left_sibling_page->RemoveAt(left_sibling_page->GetSize() - 1);
    left_sibling_guard.Drop();
    ctx->Drop();
    return internal_page->GetPageId();
  }
  auto old_size = left_sibling_page->GetSize();
  left_sibling_page->ChangeSizeBy(internal_page->GetSize());
  left_sibling_page->SetKeyAt(old_size, parent_page->KeyAt(idx));
  for (int i = 0; i < internal_page->GetSize(); i++) {
    if (i != 0) {
      left_sibling_page->SetKeyAt(old_size + i, internal_page->KeyAt(i));
    }
    left_sibling_page->SetValueAt(old_size + i, internal_page->ValueAt(i));
  }
  left_sibling_page->IncVersion();
  internal_page->IncVersion();
  parent_page->RemoveAt(idx);
  ctx->write_set_.push_back(std::move(parent_guard));
  left_sibling_guard.Drop();
  return left_sibling_page->GetPageId();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::HandleUnderflowPage(page_id_t target_page_id, KeyType &key) {
  Context ctx;
  auto guard_variant = FindLeafPagePessimistic(key, &ctx, OperationType::kHandleUnderflow);
  if (!guard_variant.has_value()) {
    return;
  }
  WritePageGuard cur_guard = std::move(guard_variant.value());
  auto leaf_page = cur_guard.AsMut<LeafPage>();
  if (!leaf_page->IsUnderflow()) {
    ctx.Drop();
    return;
  }
  if (ctx.IsRootPage(leaf_page->GetPageId())) {
    ctx.Drop();
    return;
  }

  auto page_id = leaf_page->GetPageId();
  for (auto page = cur_guard.As<BPlusTreePage>(); IsUnderflow(cur_guard.GetData(), page->IsLeafPage());) {
    if (page->IsLeafPage()) {
      page_id = MergeOrRedistributeLeafPage(&cur_guard, &ctx);
    } else {
      if (ctx.IsRootPage(cur_guard.As<InternalPage>()->GetPageId())) {
        if (page->GetSize() <= 1) {
          WritePageGuard header_guard = bpm_->WritePage(header_page_id_);
          header_guard.AsMut<BPlusTreeHeaderPage>()->root_page_id_ = page_id;
          root_page_id_ = page_id;
          ctx.Drop();
        }
        break;
      }
      page_id = MergeOrRedistributeInternalPage(&cur_guard, &ctx);
    }
    if (ctx.write_set_.empty()) {
      break;
    }
    cur_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    page = cur_guard.As<BPlusTreePage>();
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
