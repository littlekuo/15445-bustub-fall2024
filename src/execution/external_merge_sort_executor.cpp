//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.cpp
//
// Identification: src/execution/external_merge_sort_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/external_merge_sort_executor.h"
#include <iostream>
#include <optional>
#include <vector>
#include "common/config.h"
#include "execution/plans/sort_plan.h"
#include "include/execution/execution_common.h"
#include "optimizer/optimizer_internal.h"

namespace bustub {

template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), cmp_(plan->GetOrderBy()), child_executor_(std::move(child_executor)) {}

/** Initialize the external merge sort */
template <size_t K>
void ExternalMergeSortExecutor<K>::Init() {
  child_executor_->Init();
  runs_.clear();
  iterators_.clear();
  pq_ = std::priority_queue<HeapElement>();
  Tuple tuple;
  RID rid;
  std::vector<SortEntry> entries;
  BUSTUB_ASSERT(exec_ctx_->GetBufferPoolManager()->Size() > K, "Buffer pool size must be greater than K");
  // fetch all tuples from child executor
  while (child_executor_->Next(&tuple, &rid)) {
    if (entries.size() >= 2 * SortPage::CalculateMaxTuples(GetOutputSchema())) {
      std::vector<page_id_t> new_pages;
      InsertEntries(entries, new_pages);
      runs_.emplace_back(std::move(new_pages), exec_ctx_->GetBufferPoolManager());
      entries.clear();
    }
    entries.emplace_back(SortEntry{GenerateSortKey(tuple, plan_->GetOrderBy(), GetOutputSchema()), std::move(tuple)});
  }
  if (!entries.empty()) {
    std::vector<page_id_t> new_pages;
    InsertEntries(entries, new_pages);
    runs_.emplace_back(std::move(new_pages), exec_ctx_->GetBufferPoolManager());
    entries.clear();
  }
  std::cout << "runs size: " << runs_.size() << std::endl;
  while (runs_.size() > K) {
    std::vector<MergeSortRun> new_runs;
    for (size_t i = 0; i < runs_.size(); i += K) {
      std::vector<page_id_t> new_pages;
      GenerateIntermediateMergeSortRun(i, std::min(i + K, runs_.size()), new_pages);
      new_runs.emplace_back(std::move(new_pages), exec_ctx_->GetBufferPoolManager());
    }
    DeletePages(runs_);
    runs_ = std::move(new_runs);
  }
  for (auto &run : runs_) {
    iterators_.push_back(run.Begin());
  }
  for (size_t i = 0; i < iterators_.size(); ++i) {
    pq_.emplace(HeapElement{*iterators_[i], i, &cmp_, GetOutputSchema(), plan_->GetOrderBy()});
    ++iterators_[i];
  }
}

/**
 * Yield the next tuple from the external merge sort.
 * @param[out] tuple The next tuple produced by the external merge sort.
 * @param[out] rid The next tuple RID produced by the external merge sort.
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
template <size_t K>
auto ExternalMergeSortExecutor<K>::Next(Tuple *tuple, RID *rid) -> bool {
  if (pq_.empty()) {
    DeletePages(runs_);
    runs_.clear();
    return false;
  }
  const HeapElement &top = pq_.top();
  *tuple = top.entry_.second;
  auto run_index = top.run_index_;
  *rid = tuple->GetRid();
  pq_.pop();
  if (iterators_[run_index] != runs_[run_index].End()) {
    pq_.emplace(HeapElement{*iterators_[run_index], run_index, &cmp_, GetOutputSchema(), plan_->GetOrderBy()});
    ++iterators_[run_index];
  }
  return true;
}

template <size_t K>
void ExternalMergeSortExecutor<K>::InsertEntries(std::vector<SortEntry> &entries, std::vector<page_id_t> &pages) {
  std::sort(entries.begin(), entries.end(), cmp_);
  auto current_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
  WritePageGuard write_guard = exec_ctx_->GetBufferPoolManager()->WritePage(current_page_id);
  auto page = write_guard.AsMut<SortPage>();
  page->Init(GetOutputSchema());
  pages.push_back(current_page_id);
  for (auto &entry : entries) {
    if (page->Count() >= page->Capacity()) {
      current_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
      write_guard = exec_ctx_->GetBufferPoolManager()->WritePage(current_page_id);
      page = write_guard.AsMut<SortPage>();
      page->Init(GetOutputSchema());
      pages.push_back(current_page_id);
    }
    page->InsertTuple(entry.second);
  }
}

template <size_t K>
void ExternalMergeSortExecutor<K>::GenerateIntermediateMergeSortRun(size_t start, size_t end,
                                                                    std::vector<page_id_t> &new_pages) {
  std::vector<MergeSortRun::Iterator> iters;
  std::priority_queue<HeapElement> pq;
  auto current_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
  WritePageGuard write_guard = exec_ctx_->GetBufferPoolManager()->WritePage(current_page_id);
  auto page = write_guard.AsMut<SortPage>();
  page->Init(GetOutputSchema());
  new_pages.push_back(current_page_id);
  for (size_t i = start; i < end; ++i) {
    auto it = runs_[i].Begin();
    pq.push(HeapElement{*it, i, &cmp_, GetOutputSchema(), plan_->GetOrderBy()});
    iters.push_back(std::move(++it));
  }
  while (!pq.empty()) {
    const HeapElement &top = pq.top();
    if (page->Count() >= page->Capacity()) {
      current_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
      write_guard = exec_ctx_->GetBufferPoolManager()->WritePage(current_page_id);
      page = write_guard.AsMut<SortPage>();
      page->Init(GetOutputSchema());
      new_pages.push_back(current_page_id);
    }
    page->InsertTuple(top.entry_.second);
    auto run_index = top.run_index_;
    pq.pop();
    if (iters[run_index - start] != runs_[run_index].End()) {
      pq.emplace(HeapElement{*iters[run_index - start], run_index, &cmp_, GetOutputSchema(), plan_->GetOrderBy()});
      ++iters[run_index - start];
    }
  }
}

template <size_t K>
void ExternalMergeSortExecutor<K>::DeletePages(std::vector<MergeSortRun> &runs) {
  for (auto &run : runs) {
    for (auto page_id : run.GetPages()) {
      exec_ctx_->GetBufferPoolManager()->DeletePage(page_id);
    }
  }
}

template class ExternalMergeSortExecutor<2>;

}  // namespace bustub
