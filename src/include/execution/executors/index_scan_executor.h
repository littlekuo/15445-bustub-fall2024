//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */

class IndexScanExecutor : public AbstractExecutor {
 public:
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

 private:
  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;
  BPlusTreeIndexForTwoIntegerColumn *index_;
  BPlusTreeIndexIteratorForTwoIntegerColumn index_iterator_;
  bool is_point_lookup_{false};

  // the remaining_conds_ of IndexScanPlanNode should be considered

  // valid for point lookup
  /** for example, if the index is on (a, b) and the query is `(a = 1 and b = 2) or (a = 3 and b = 4)`,
   * then the point lookup keys are {[1, 2], [3, 4]}
   */
  std::vector<Tuple> point_lookup_tuples_;
  size_t point_lookup_idx_{0};

  // valid for range lookup
  /** for example, if the index is on (a, b, c, d) and the query is `a = 1 and b = 2 and c > 3`,
   *  then the start key is [1, 2, 3, MIN] and the prefix predicates are `a = 1 and b = 2 and c > 3`,
   *  we use the start key to seek the index, if prefix predicates are not fit, we should
   *  just stop the scan.
   */
  Tuple start_tuple_;
  std::vector<AbstractExpressionRef> prefix_preds_;
};
}  // namespace bustub
