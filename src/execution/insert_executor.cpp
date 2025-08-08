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
  int32_t count = 0;
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  std::vector<Tuple> tuples;
  while (child_executor_->Next(tuple, rid)) {
    tuples.emplace_back(std::move(*tuple));
  }
  for (auto &tuple : tuples) {
    auto rid_opt = table_info_->table_->InsertTuple({0, false}, tuple, exec_ctx_->GetLockManager(),
                                                    exec_ctx_->GetTransaction(), plan_->GetTableOid());
    auto rid_val = rid_opt.value();  // 安全获取RID值
    std::cout << "Inserted tuple with RID: " << rid_val << std::endl;

    for (auto index : indexes) {
      index->index_->InsertEntry(tuple, rid_val, exec_ctx_->GetTransaction());
    }
    count++;
  }
  *tuple = Tuple({Value(TypeId::INTEGER, count)}, &GetOutputSchema());
  inserted_ = true;
  return true;
}

}  // namespace bustub
