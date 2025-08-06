//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.h
//
// Identification: src/include/execution/executors/external_merge_sort_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <queue>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

#define SORT_PAGE_SIZE ((BUSTUB_PAGE_SIZE - 16))

struct HeapElement {
  size_t run_index_;
  const TupleComparator *cmp_;
  SortEntry entry_;

  HeapElement(Tuple tuple, size_t run_index, const TupleComparator *cmp, const Schema &schema,
              const std::vector<OrderBy> &order_bys)
      : run_index_(run_index), cmp_(cmp) {
    entry_ = std::make_pair(GenerateSortKey(tuple, order_bys, schema), std::move(tuple));
  }

  auto operator<(const HeapElement &other) const -> bool { return (*cmp_)(other.entry_, entry_); }
};

/**
 * Page to hold the intermediate data for external merge sort.
 *
 * Only fixed-length data will be supported in Fall 2024.
 */
class SortPage {
 public:
  /**
   * TODO: Define and implement the methods for reading data from and writing data to the sort
   * page. Feel free to add other helper methods.
   */
  void Init(const Schema &schema) {
    count_ = 0;
    tuple_size_ = sizeof(RID) + schema.GetInlinedStorageSize();
    BUSTUB_ASSERT(schema.GetUnlinedColumnCount() == 0, "only fixed-length data is supported");
    if (tuple_size_ == 0) {
      cap_ = 0;
      return;
    }
    cap_ = SORT_PAGE_SIZE / tuple_size_;
  }

  auto InsertTuple(const Tuple &tuple) -> bool {
    if (count_ >= cap_) {
      return false;
    }
    char *dest_ptr = data_ + count_ * tuple_size_;
    auto rid = tuple.GetRid();
    memcpy(dest_ptr, &rid, sizeof(RID));
    memcpy(dest_ptr + sizeof(RID), tuple.GetData(), tuple_size_ - sizeof(RID));
    count_++;
    return true;
  }

  // only fixed-length data is supported
  void GetTuple(uint32_t idx, Tuple *tuple) const {
    BUSTUB_ASSERT(idx < count_, "idx out of range");
    const char *src_ptr = data_ + idx * tuple_size_;
    RID rid;
    memcpy(&rid, src_ptr, sizeof(RID));
    *tuple = Tuple(rid, src_ptr + sizeof(RID), tuple_size_ - sizeof(RID));
  }

  auto Count() const -> uint32_t { return count_; }
  auto Capacity() const -> uint32_t { return cap_; }

  static auto CalculateMaxTuples(const Schema &schema) -> uint32_t {
    return SORT_PAGE_SIZE / (schema.GetInlinedStorageSize() + sizeof(RID));
  }

 private:
  /**
   * TODO: Define the private members. You may want to have some necessary metadata for
   * the sort page before the start of the actual data.
   */
  uint32_t count_{0};
  uint32_t cap_{0};
  uint32_t tuple_size_{0};
  char data_[SORT_PAGE_SIZE];
};

/**
 * A data structure that holds the sorted tuples as a run during external merge sort.
 * Tuples might be stored in multiple pages, and tuples are ordered both within one page
 * and across pages.
 */
class MergeSortRun {
 public:
  MergeSortRun() = default;
  MergeSortRun(std::vector<page_id_t> pages, BufferPoolManager *bpm) : pages_(std::move(pages)), bpm_(bpm) {}

  auto GetPageCount() -> size_t { return pages_.size(); }
  auto GetPages() -> const std::vector<page_id_t> & { return pages_; }

  /** Iterator for iterating on the sorted tuples in one run. */
  class Iterator {
    friend class MergeSortRun;

   public:
    Iterator() = default;

    /**
     * Advance the iterator to the next tuple. If the current sort page is exhausted, move to the
     * next sort page.
     *
     * TODO: Implement this method.
     */
    auto operator++() -> Iterator & {
      while (true) {
        if (page_idx_ >= run_->pages_.size()) {
          is_end_ = true;
          return *this;
        }
        if (tuple_idx_ >= guard_.As<SortPage>()->Count() - 1) {
          page_idx_++;
          if (page_idx_ >= run_->pages_.size()) {
            is_end_ = true;
            return *this;
          }
          guard_ = run_->bpm_->ReadPage(run_->pages_[page_idx_]);
          tuple_idx_ = 0;
          return *this;
        }
        tuple_idx_++;
        return *this;
      }
    }

    /**
     * Dereference the iterator to get the current tuple in the sorted run that the iterator is
     * pointing to.
     *
     * TODO: Implement this method.
     */
    auto operator*() -> Tuple {
      Tuple tuple;
      guard_.As<SortPage>()->GetTuple(tuple_idx_, &tuple);
      return tuple;
    }

    /**
     * Checks whether two iterators are pointing to the same tuple in the same sorted run.
     *
     * TODO: Implement this method.
     */
    auto operator==(const Iterator &other) const -> bool {
      if (is_end_ || other.is_end_) {
        return is_end_ == other.is_end_;
      }
      return run_ == other.run_ && page_idx_ == other.page_idx_ && tuple_idx_ == other.tuple_idx_;
    }

    /**
     * Checks whether two iterators are pointing to different tuples in a sorted run or iterating
     * on different sorted runs.
     *
     * TODO: Implement this method.
     */
    auto operator!=(const Iterator &other) const -> bool { return !(*this == other); }

   private:
    explicit Iterator(const MergeSortRun *run) : run_(run) {
      if (!run_->pages_.empty()) {
        guard_ = run_->bpm_->ReadPage(run_->pages_[0]);
      }
      is_end_ = run_->pages_.empty();
    }

    /** The sorted run that the iterator is iterating on. */
    const MergeSortRun *run_{nullptr};

    /**
     * TODO: Add your own private members here. You may want something to record your current
     * position in the sorted run. Also feel free to add additional constructors to initialize
     * your private members.
     */
    uint32_t page_idx_{0};
    uint32_t tuple_idx_{0};
    bool is_end_{false};
    ReadPageGuard guard_{};
  };

  /**
   * Get an iterator pointing to the beginning of the sorted run, i.e. the first tuple.
   *
   * TODO: Implement this method.
   */
  auto Begin() -> Iterator { return Iterator(this); }

  /**
   * Get an iterator pointing to the end of the sorted run, i.e. the position after the last tuple.
   *
   * TODO: Implement this method.
   */
  auto End() -> Iterator {
    Iterator it;
    it.is_end_ = true;
    return it;
  }

 private:
  /** The page IDs of the sort pages that store the sorted tuples. */
  std::vector<page_id_t> pages_;
  /**
   * The buffer pool manager used to read sort pages. The buffer pool manager is responsible for
   * deleting the sort pages when they are no longer needed.
   */
  BufferPoolManager *bpm_;
};

/**
 * ExternalMergeSortExecutor executes an external merge sort.
 *
 * In Fall 2024, only 2-way external merge sort is required.
 */
template <size_t K>
class ExternalMergeSortExecutor : public AbstractExecutor {
 public:
  ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                            std::unique_ptr<AbstractExecutor> &&child_executor);

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the external merge sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void InsertEntries(std::vector<SortEntry> &entries, std::vector<page_id_t> &pages);

  void GenerateIntermediateMergeSortRun(size_t start, size_t end, std::vector<page_id_t> &new_pages);

  void DeletePages(std::vector<MergeSortRun> &runs);

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;

  /** Compares tuples based on the order-bys */
  TupleComparator cmp_;

  /** TODO: You will want to add your own private members here. */
  std::unique_ptr<AbstractExecutor> child_executor_{};
  std::vector<MergeSortRun> runs_{};
  std::vector<MergeSortRun::Iterator> iterators_{};
  std::priority_queue<HeapElement> pq_{};
};

}  // namespace bustub
