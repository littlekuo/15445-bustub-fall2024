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
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
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
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  iter_ = nullptr;
  if (table_info_ != nullptr) {
    iter_ = std::make_unique<TableIterator>(table_info_->table_->MakeIterator());
    AbstractExpressionRef dummy_pred = std::make_shared<ConstantValueExpression>(Value(BOOLEAN, 1));
    exec_ctx_->GetTransaction()->AppendScanPredicate(
        plan_->GetTableOid(), plan_->filter_predicate_ != nullptr ? plan_->filter_predicate_ : dummy_pred);
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
    // ensure atomic read
    auto cur_rid = iter_->GetRID();
    auto [tuple_meta, tuple_, undo_link] =
        GetTupleAndUndoLink(exec_ctx_->GetTransactionManager(), table_info_->table_.get(), cur_rid);
    auto undo_logs = CollectUndoLogs(iter_->GetRID(), tuple_meta, tuple_, undo_link, exec_ctx_->GetTransaction(),
                                     exec_ctx_->GetTransactionManager());
    if (!undo_logs.has_value()) {
      // means tuple is not exist at that time
      ++(*iter_);
      continue;
    }
    auto rebuild_tuple = ReconstructTuple(&GetOutputSchema(), tuple_, tuple_meta, undo_logs.value());
    if (!rebuild_tuple.has_value()) {
      // means tuple is deleted
      ++(*iter_);
      continue;
    }
    rebuild_tuple->SetRid(cur_rid);
    tuple_ = rebuild_tuple.value();
    if (plan_->filter_predicate_ != nullptr) {
      auto value = plan_->filter_predicate_->Evaluate(&tuple_, GetOutputSchema());
      if (value.IsNull() || !value.GetAs<bool>()) {
        ++(*iter_);
        continue;
      }
    }

    *tuple = tuple_;
    *rid = cur_rid;
    ++(*iter_);
    break;
  }
  return true;
}

}  // namespace bustub
