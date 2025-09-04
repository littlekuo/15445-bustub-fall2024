//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

/**
 * Construct a new InsertExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled
 */
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the insert */
void InsertExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  inserted_ = false;
}

/**
 * Yield the number of rows inserted into the table.
 * @param[out] tuple The integer tuple indicating the number of rows inserted into the table
 * @param[out] rid The next tuple RID produced by the insert (ignore, not used)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: InsertExecutor::Next() does not use the `rid` out-parameter.
 * NOTE: InsertExecutor::Next() returns true with number of inserted rows produced only once.
 */
auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (inserted_) {
    return false;
  }
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  std::vector<Tuple> tuples;
  while (child_executor_->Next(tuple, rid)) {
    tuples.emplace_back(std::move(*tuple));
  }
  for (auto &cur_tuple : tuples) {
    std::optional<RID> rid_opt = std::nullopt;
    for (const auto &index : indexes) {
      // only consider primary key
      if (!index->is_primary_key_) {
        continue;
      }
      auto ret = CheckKeyIfExistInIndex(&table_info_->schema_, &cur_tuple, index.get(), exec_ctx_->GetTransaction(),
                                        table_info_.get());
      if (ret.first) {
        exec_ctx_->GetTransaction()->SetTainted();
        throw ExecutionException("Duplicate key");
      }
      if (ret.second.GetPageId() != INVALID_PAGE_ID) {
        rid_opt = ret.second;
      }
    }
    InsertTupleAndIndexKey(&cur_tuple, exec_ctx_->GetTransactionManager(), exec_ctx_->GetTransaction(),
                           table_info_.get(), rid_opt, exec_ctx_->GetLockManager(), indexes);
    exec_ctx_->GetTransaction()->AppendWriteSet(table_info_->oid_, rid_opt.value());
  }
  *tuple = Tuple({Value(TypeId::INTEGER, static_cast<int32_t>(tuples.size()))}, &GetOutputSchema());
  inserted_ = true;
  return true;
}

}  // namespace bustub
