//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

/**
 * Construct a new DeleteExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The delete plan to be executed
 * @param child_executor The child executor that feeds the delete
 */
DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the delete */
void DeleteExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid()).get();
}

/**
 * Yield the number of rows deleted from the table.
 * @param[out] tuple The integer tuple indicating the number of rows deleted from the table
 * @param[out] rid The next tuple RID produced by the delete (ignore, not used)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: DeleteExecutor::Next() does not use the `rid` out-parameter.
 * NOTE: DeleteExecutor::Next() returns true with the number of deleted rows produced only once.
 */
auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (deleted_) {
    return false;
  }

  std::vector<Tuple> tuples;
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  // collect old tuples and detect conflict
  while (child_executor_->Next(tuple, rid)) {
    auto tuple_meta = table_info_->table_->GetTupleMeta(*rid);
    if (IsWriteWriteConflict(exec_ctx_->GetTransaction(), tuple_meta.ts_)) {
      exec_ctx_->GetTransaction()->SetTainted();
      throw ExecutionException("w-w conflict with other committed txn");
    }
    tuples.emplace_back(*tuple);
  }
  for (auto &tuple : tuples) {
    auto tuple_link_info =
        GetTupleAndUndoLink(exec_ctx_->GetTransactionManager(), table_info_->table_.get(), tuple.GetRid());
    auto tuple_meta = std::get<0>(tuple_link_info);
    if (IsWriteWriteConflict(exec_ctx_->GetTransaction(), tuple_meta.ts_)) {
      exec_ctx_->GetTransaction()->SetTainted();
      throw ExecutionException("w-w conflict with other committed txn");
    }
    BUSTUB_ASSERT(!tuple_meta.is_deleted_, "Cannot delete a tuple that is already deleted");
    auto target_undo_link =
        GenerateOrFindUndoLink(&table_info_->schema_, exec_ctx_->GetTransactionManager(), exec_ctx_->GetTransaction(),
                               &tuple, tuple_meta.ts_, nullptr, std::get<2>(tuple_link_info));
    auto cur_ts = tuple_meta.ts_;
    auto check = [cur_ts](const TupleMeta &meta, const Tuple &tuple, RID rid, std::optional<UndoLink>) {
      return meta.ts_ == cur_ts;
    };
    tuple_meta.ts_ = exec_ctx_->GetTransaction()->GetTransactionTempTs();
    tuple_meta.is_deleted_ = true;
    auto updated =
        UpdateTupleAndUndoLink(exec_ctx_->GetTransactionManager(), tuple.GetRid(), target_undo_link,
                               table_info_->table_.get(), exec_ctx_->GetTransaction(), tuple_meta, tuple, check);
    if (!updated) {
      exec_ctx_->GetTransaction()->SetTainted();
      throw ExecutionException("Delete failed");
    }
    exec_ctx_->GetTransaction()->AppendWriteSet(table_info_->oid_, tuple.GetRid());
    // don't update primary index to support read history tuple (only consider primary key)
  }
  *tuple = Tuple({Value(TypeId::INTEGER, static_cast<int32_t>(tuples.size()))}, &GetOutputSchema());
  deleted_ = true;
  return true;
}

}  // namespace bustub
