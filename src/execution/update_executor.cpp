//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

/**
 * Construct a new UpdateExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The update plan to be executed
 * @param child_executor The child executor that feeds the update
 */
UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan->GetTableOid()).get();
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

/** Initialize the update */
void UpdateExecutor::Init() { child_executor_->Init(); }

/**
 * Yield the next tuple from the update.
 * @param[out] tuple The next tuple produced by the update
 * @param[out] rid The next tuple RID produced by the update (ignore this)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: UpdateExecutor::Next() does not use the `rid` out-parameter.
 */
auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (updated_) {
    return false;
  }
  int32_t count = 0;
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  std::vector<Tuple> tuples;
  while (child_executor_->Next(tuple, rid)) {
    // delete old tuple
    auto tuple_meta = table_info_->table_->GetTupleMeta(*rid);
    tuple_meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(tuple_meta, *rid);
    for (auto &index : indexes) {
      index->index_->DeleteEntry(
          tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()), tuple->GetRid(),
          exec_ctx_->GetTransaction());
    }

    // collect new tuple
    std::vector<Value> values;
    for (auto &expr : plan_->target_expressions_) {
      values.emplace_back(expr->Evaluate(tuple, table_info_->schema_));
    }
    tuples.emplace_back(Tuple(values, &table_info_->schema_));
  }

  for (auto &tuple : tuples) {
    auto rid_opt = table_info_->table_->InsertTuple({0, false}, tuple, exec_ctx_->GetLockManager(),
                                                    exec_ctx_->GetTransaction(), plan_->GetTableOid());
    for (auto &index : indexes) {
      index->index_->InsertEntry(
          tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()), rid_opt.value(),
          exec_ctx_->GetTransaction());
    }
    count++;
  }

  *tuple = Tuple({Value(TypeId::INTEGER, count)}, &GetOutputSchema());
  updated_ = true;
  return true;
}

}  // namespace bustub
