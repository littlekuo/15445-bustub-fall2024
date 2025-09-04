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

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
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
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  std::vector<RID> tuple_rids;
  std::vector<Tuple> new_tuples;
  while (child_executor_->Next(tuple, rid)) {
    // collect tuple_ids
    auto tuple_meta = table_info_->table_->GetTupleMeta(*rid);
    if (IsWriteWriteConflict(exec_ctx_->GetTransaction(), tuple_meta.ts_)) {
      exec_ctx_->GetTransaction()->SetTainted();
      throw ExecutionException("w-w conflict with other txn");
    }
    tuple_rids.emplace_back(*rid);
    std::vector<Value> values;
    for (auto &expr : plan_->target_expressions_) {
      values.emplace_back(expr->Evaluate(tuple, table_info_->schema_));
    }
    new_tuples.emplace_back(Tuple(values, &table_info_->schema_));
  }

  // only consider primary key (P4)
  std::vector<bool> updated;
  updated.resize(tuple_rids.size(), false);
  for (size_t i = 0; i < tuple_rids.size(); i++) {
    auto tuple_link_info =
        GetTupleAndUndoLink(exec_ctx_->GetTransactionManager(), table_info_->table_.get(), tuple_rids[i]);
    auto tuple_meta = std::get<0>(tuple_link_info);
    auto old_tuple = std::get<1>(tuple_link_info);
    auto new_tuple = new_tuples[i];
    // check if w-w conflict
    if (IsWriteWriteConflict(exec_ctx_->GetTransaction(), tuple_meta.ts_)) {
      exec_ctx_->GetTransaction()->SetTainted();
      throw ExecutionException("w-w conflict with other txn");
    }
    // 0. check primary key if equal
    bool primary_key_equal = true;
    for (auto &index : indexes) {
      if (!index->is_primary_key_) {
        continue;
      }
      auto old_key = old_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      auto new_key = new_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      for (size_t i = 0; i < index->key_schema_.GetColumnCount(); i++) {
        if (!old_key.GetValue(&index->key_schema_, i).CompareExactlyEquals(new_key.GetValue(&index->key_schema_, i))) {
          primary_key_equal = false;
          break;
        }
      }
    }

    if (primary_key_equal) {
      // just update the tuple in place
      auto new_undo_link =
          GenerateOrFindUndoLink(&table_info_->schema_, exec_ctx_->GetTransactionManager(), exec_ctx_->GetTransaction(),
                                 &old_tuple, tuple_meta.ts_, &new_tuple, std::get<2>(tuple_link_info));
      auto cur_ts = tuple_meta.ts_;
      auto check = [cur_ts](const TupleMeta &meta, const Tuple &tuple, RID rid, std::optional<UndoLink>) {
        return meta.ts_ == cur_ts;
      };
      tuple_meta.ts_ = exec_ctx_->GetTransaction()->GetTransactionTempTs();
      auto is_updated =
          UpdateTupleAndUndoLink(exec_ctx_->GetTransactionManager(), old_tuple.GetRid(), new_undo_link,
                                 table_info_->table_.get(), exec_ctx_->GetTransaction(), tuple_meta, new_tuple, check);
      if (!is_updated) {
        exec_ctx_->GetTransaction()->SetTainted();
        throw ExecutionException("update tuple failed");
      }
      exec_ctx_->GetTransaction()->AppendWriteSet(table_info_->oid_, tuple_rids[i]);
      updated[i] = true;
      continue;
    }

    // need to delete old and insert new
    // 1. delete old tuple
    auto new_undo_link =
        GenerateOrFindUndoLink(&table_info_->schema_, exec_ctx_->GetTransactionManager(), exec_ctx_->GetTransaction(),
                               &old_tuple, tuple_meta.ts_, nullptr, std::get<2>(tuple_link_info));
    auto cur_ts = tuple_meta.ts_;
    auto check = [cur_ts](const TupleMeta &meta, const Tuple &tuple, RID rid, std::optional<UndoLink>) {
      return meta.ts_ == cur_ts;
    };
    tuple_meta.ts_ = exec_ctx_->GetTransaction()->GetTransactionTempTs();
    tuple_meta.is_deleted_ = true;
    auto is_deleted =
        UpdateTupleAndUndoLink(exec_ctx_->GetTransactionManager(), old_tuple.GetRid(), new_undo_link,
                               table_info_->table_.get(), exec_ctx_->GetTransaction(), tuple_meta, old_tuple, check);
    if (!is_deleted) {
      exec_ctx_->GetTransaction()->SetTainted();
      throw ExecutionException("Delete failed");
    }

    // 2. check key if exist
    std::optional<RID> rid_opt = std::nullopt;
    bool key_exist = false;
    for (auto &index : indexes) {
      if (!index->is_primary_key_) {
        continue;
      }
      auto ret = CheckKeyIfExistInIndex(&table_info_->schema_, &new_tuples[i], index.get(), exec_ctx_->GetTransaction(),
                                        table_info_);
      if (ret.first) {
        key_exist = true;
        break;
      }
      if (ret.second.GetPageId() != INVALID_PAGE_ID) {
        rid_opt = ret.second;
      }
    }

    if (key_exist) {
      // retry it later
      continue;
    }

    // 3. insert new tuple
    InsertTupleAndIndexKey(&new_tuples[i], exec_ctx_->GetTransactionManager(), exec_ctx_->GetTransaction(), table_info_,
                           rid_opt, exec_ctx_->GetLockManager(), indexes);
    exec_ctx_->GetTransaction()->AppendWriteSet(table_info_->oid_, rid_opt.value());
    exec_ctx_->GetTransaction()->AppendWriteSet(table_info_->oid_, tuple_rids[i]);
    updated[i] = true;
  }

  // 4. try to insert for previous not updated
  for (size_t i = 0; i < tuple_rids.size(); i++) {
    if (!updated[i]) {
      std::optional<RID> rid_opt = std::nullopt;
      for (auto &index : indexes) {
        if (!index->is_primary_key_) {
          continue;
        }
        auto ret = CheckKeyIfExistInIndex(&table_info_->schema_, &new_tuples[i], index.get(),
                                          exec_ctx_->GetTransaction(), table_info_);
        if (ret.first) {
          exec_ctx_->GetTransaction()->SetTainted();
          throw ExecutionException("key conflict");
        }
        if (ret.second.GetPageId() != INVALID_PAGE_ID) {
          rid_opt = ret.second;
        }
      }
      assert(rid_opt.has_value());
      InsertTupleAndIndexKey(&new_tuples[i], exec_ctx_->GetTransactionManager(), exec_ctx_->GetTransaction(),
                             table_info_, rid_opt, exec_ctx_->GetLockManager(), indexes);
      updated[i] = true;
      exec_ctx_->GetTransaction()->AppendWriteSet(table_info_->oid_, rid_opt.value());
      exec_ctx_->GetTransaction()->AppendWriteSet(table_info_->oid_, tuple_rids[i]);
    }
  }

  *tuple = Tuple({Value(TypeId::INTEGER, static_cast<int32_t>(tuple_rids.size()))}, &GetOutputSchema());
  updated_ = true;
  return true;
}

}  // namespace bustub
