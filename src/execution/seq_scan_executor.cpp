//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "optimizer/optimizer.h"
#include "optimizer/optimizer_internal.h"

namespace bustub {

/**
 * Construct a new SeqScanExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The sequential scan plan to be executed
 */
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

/** Initialize the sequential scan */
void SeqScanExecutor::Init() {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  iter_ = nullptr;
  if (table_info != nullptr) {
    iter_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());
    return;
  }
  throw bustub::Exception("table not found");
}

/**
 * Yield the next tuple from the sequential scan.
 * @param[out] tuple The next tuple produced by the scan
 * @param[out] rid The next tuple RID produced by the scan
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (IsPredicateFalse(plan_->filter_predicate_)) {
    return false;
  }
  if (iter_->IsEnd()) {
    return false;
  }
  while (true) {
    if (iter_->IsEnd()) {
      return false;
    }
    auto [tuple_meta, tuple_] = iter_->GetTuple();
    if (tuple_meta.is_deleted_) {
      ++(*iter_);
      continue;
    }
    if (plan_->filter_predicate_ != nullptr) {
      auto value = plan_->filter_predicate_->Evaluate(&tuple_, GetOutputSchema());
      if (value.IsNull() || !value.GetAs<bool>()) {
        ++(*iter_);
        continue;
      }
    }

    *tuple = tuple_;
    *rid = iter_->GetRID();
    ++(*iter_);
    break;
  }
  return true;
}

}  // namespace bustub
