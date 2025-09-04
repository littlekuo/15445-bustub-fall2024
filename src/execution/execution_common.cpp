//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_common.cpp
//
// Identification: src/execution/execution_common.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/execution_common.h"

#include "catalog/catalog.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

TupleComparator::TupleComparator(std::vector<OrderBy> order_bys) : order_bys_(std::move(order_bys)) {}

/** TODO(P3): Implement the comparison method */
auto TupleComparator::operator()(const SortEntry &entry_a, const SortEntry &entry_b) const -> bool {
  for (size_t i = 0; i < order_bys_.size(); i++) {
    auto res = entry_a.first[i].CompareEquals(entry_b.first[i]);
    if (res == CmpBool::CmpTrue) {
      continue;
    }
    res = entry_a.first[i].CompareLessThan(entry_b.first[i]);
    OrderByType order = order_bys_[i].first;
    if (order == OrderByType::DEFAULT) {
      order = OrderByType::ASC;
    }
    if (res == CmpBool::CmpTrue) {
      return order == OrderByType::ASC;
    }
    return order == OrderByType::DESC;
  }
  return false;
}

/**
 * Generate sort key for a tuple based on the order by expressions.
 *
 * TODO(P3): Implement this method.
 */
auto GenerateSortKey(const Tuple &tuple, const std::vector<OrderBy> &order_bys, const Schema &schema) -> SortKey {
  SortKey key;
  key.reserve(order_bys.size());
  for (const auto &order_by : order_bys) {
    key.emplace_back(order_by.second->Evaluate(&tuple, schema));
  }
  return key;
}

/**
 * Above are all you need for P3.
 * You can ignore the remaining part of this file until P4.
 */

/**
 * @brief Reconstruct a tuple by applying the provided undo logs from the base tuple. All logs in the undo_logs are
 * applied regardless of the timestamp
 *
 * @param schema The schema of the base tuple and the returned tuple.
 * @param base_tuple The base tuple to start the reconstruction from.
 * @param base_meta The metadata of the base tuple.
 * @param undo_logs The list of undo logs to apply during the reconstruction, the front is applied first.
 * @return An optional tuple that represents the reconstructed tuple. If the tuple is deleted as the result, returns
 * std::nullopt.
 */
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  std::vector<Value> values;
  for (size_t i = 0; i < schema->GetColumnCount(); i++) {
    values.push_back(base_tuple.GetValue(schema, i));
  }
  bool is_deleted = base_meta.is_deleted_;
  for (const auto &undo_log : undo_logs) {
    if (undo_log.is_deleted_) {
      is_deleted = true;
      values.clear();
      continue;
    }
    is_deleted = false;
    if (values.empty()) {
      // previous undo_log is delete, current undo_log.tuple should be the full tuple
      for (size_t i = 0; i < schema->GetColumnCount(); i++) {
        BUSTUB_ASSERT(i < undo_log.modified_fields_.size() && undo_log.modified_fields_[i], "index out of bounds");
        values.push_back(undo_log.tuple_.GetValue(schema, i));
      }
    } else {
      auto valid_schema = GetUndoLogSchema(schema, undo_log);
      auto idx = 0;
      for (size_t i = 0; i < undo_log.modified_fields_.size(); i++) {
        if (undo_log.modified_fields_[i]) {
          values[i] = undo_log.tuple_.GetValue(&valid_schema, idx++);
        }
      }
    }
  }
  if (is_deleted) {
    return std::nullopt;
  }
  BUSTUB_ASSERT(!values.empty(), "values vector should not be empty");
  return Tuple{values, schema};
}

/**
 * @brief Collects the undo logs sufficient to reconstruct the tuple w.r.t. the txn.
 *
 * @param rid The RID of the tuple.
 * @param base_meta The metadata of the base tuple.
 * @param base_tuple The base tuple.
 * @param undo_link The undo link to the latest undo log.
 * @param txn The transaction.
 * @param txn_mgr The transaction manager.
 * @return An optional vector of undo logs to pass to ReconstructTuple(). std::nullopt if the tuple did not exist at the
 * time.
 */
auto CollectUndoLogs(RID rid, const TupleMeta &base_meta, const Tuple &base_tuple, std::optional<UndoLink> undo_link,
                     Transaction *txn, TransactionManager *txn_mgr) -> std::optional<std::vector<UndoLog>> {
  std::vector<UndoLog> undo_logs;
  if (base_meta.ts_ == txn->GetTransactionTempTs()) {
    return undo_logs;
  }
  if (base_meta.ts_ <= txn->GetReadTs()) {
    return undo_logs;
  }
  bool is_find = false;
  while (undo_link.has_value() && undo_link->IsValid()) {
    auto undo_log_opt = txn_mgr->GetUndoLogOptional(*undo_link);
    if (!undo_log_opt.has_value()) {
      break;
    }
    auto undo_log = undo_log_opt.value();
    if (undo_log.ts_ == txn->GetTransactionTempTs()) {
      break;
    }
    if (undo_log.ts_ <= txn->GetReadTs()) {
      is_find = true;
      undo_logs.push_back(undo_log);
      break;
    }
    undo_logs.push_back(undo_log);
    undo_link = undo_log.prev_version_;
  }
  if (!is_find) {
    return std::nullopt;
  }
  return undo_logs;
}

/**
 * @brief Generates a new undo log as the transaction tries to modify this tuple at the first time.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param ts The timestamp of the base tuple.
 * @param prev_version The undo link to the latest undo log of this tuple.
 * @return The generated undo log.
 */
auto GenerateNewUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple, timestamp_t ts,
                        UndoLink prev_version) -> UndoLog {
  if (base_tuple == nullptr) {
    // insert operation
    return UndoLog{true, {}, {}, ts, prev_version};
  }
  if (target_tuple == nullptr) {
    // delete operation, tuple is just original tuple
    std::vector<bool> modified_fields;
    modified_fields.resize(schema->GetColumnCount(), true);
    auto original_tuple = *base_tuple;
    return UndoLog{false, modified_fields, original_tuple, ts, prev_version};
  }
  std::vector<bool> modified_fields;
  modified_fields.resize(schema->GetColumnCount(), false);
  std::vector<Column> modified_columns;
  std::vector<Value> modified_values;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    if (base_tuple->GetValue(schema, i).CompareExactlyEquals(target_tuple->GetValue(schema, i))) {
      continue;
    }
    modified_fields[i] = true;
    modified_columns.push_back(schema->GetColumn(i));
    modified_values.push_back(base_tuple->GetValue(schema, i));
  }
  Schema modified_schema(modified_columns);
  Tuple tuple(modified_values, &modified_schema);
  return UndoLog{false, modified_fields, std::move(tuple), ts, prev_version};
}

/**
 * @brief Generate the updated undo log to replace the old one, whereas the tuple is already modified by this txn once.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param log The original undo log.
 * @return The updated undo log.
 */
auto GenerateUpdatedUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple,
                            const UndoLog &log) -> UndoLog {
  if (log.is_deleted_) {
    return log;
  }
  if (base_tuple == nullptr) {
    return GenerateNewUndoLog(schema, &log.tuple_, target_tuple, log.ts_, log.prev_version_);
  }
  auto original_tuple = ReconstructTuple(schema, *base_tuple, {0, false}, {log});
  BUSTUB_ASSERT(original_tuple.has_value(), "Reconstructed tuple should have a value");
  auto cur_log = GenerateNewUndoLog(schema, base_tuple, target_tuple, log.ts_, log.prev_version_);
  UndoLog combined_log;
  combined_log.is_deleted_ = log.is_deleted_;
  combined_log.modified_fields_.resize(schema->GetColumnCount(), false);
  combined_log.ts_ = log.ts_;
  combined_log.prev_version_ = log.prev_version_;
  std::vector<Value> values;
  for (size_t i = 0; i < schema->GetColumnCount(); i++) {
    if (cur_log.modified_fields_[i] || log.modified_fields_[i]) {
      combined_log.modified_fields_[i] = true;
      values.push_back(original_tuple->GetValue(schema, i));
    }
  }
  auto temp_schema = GetUndoLogSchema(schema, combined_log);
  combined_log.tuple_ = Tuple(values, &temp_schema);
  return combined_log;
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  // fmt::println(
  //     stderr,
  //     "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
  //     "finished task 2. Implementing this helper function will save you a lot of time for debugging in later
  //     tasks.");

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
  auto iter = table_info->table_->MakeIterator();
  while (!iter.IsEnd()) {
    auto [tuple_meta, tuple_] = iter.GetTuple();
    fmt::println(stderr, "RID={}/{} ts={} {} tuple={}", tuple_.GetRid().GetPageId(), tuple_.GetRid().GetSlotNum(),
                 tuple_meta.ts_, tuple_meta.is_deleted_ ? "<del marker>" : "", tuple_.ToString(&table_info->schema_));
    auto undo_link = txn_mgr->GetUndoLink(tuple_.GetRid());
    while (undo_link.has_value() && undo_link->IsValid()) {
      auto undo_log_opt = txn_mgr->GetUndoLogOptional(undo_link.value());
      if (!undo_log_opt.has_value()) {
        break;
      }
      auto undo_log = undo_log_opt.value();
      if (undo_log.is_deleted_) {
        fmt::println(stderr, "  txn{}@{} <del> ts={}", undo_link->prev_txn_ ^ TXN_START_ID, undo_link->prev_log_idx_,
                     undo_log.ts_);
        undo_link = undo_log.prev_version_;
        continue;
      }
      std::vector<Value> values;
      std::vector<Column> columns;
      auto undo_log_schema = GetUndoLogSchema(&table_info->schema_, undo_log);
      auto col_idx = 0;
      for (size_t i = 0; i < table_info->schema_.GetColumnCount(); ++i) {
        if (undo_log.modified_fields_[i]) {
          columns.emplace_back(table_info->schema_.GetColumn(i));
          values.emplace_back(undo_log.tuple_.GetValue(&undo_log_schema, col_idx++));
        } else {
          columns.emplace_back(Column("NULL", TypeId::VARCHAR, 5));
          values.emplace_back(ValueFactory::GetVarcharValue("_"));
        }
      }
      Schema schema(columns);
      Tuple tuple(values, &schema);
      fmt::println(stderr, "  txn{}@{} {} ts={}", undo_link->prev_txn_ ^ TXN_START_ID, undo_link->prev_log_idx_,
                   tuple.ToString(&schema), undo_log.ts_);
      undo_link = undo_log.prev_version_;
    }
    ++iter;
  }
}

auto IsWriteWriteConflict(Transaction *txn, timestamp_t ts) -> bool {
  if (ts >= TXN_START_ID && ts != txn->GetTransactionTempTs()) {
    return true;
  }
  if (ts < TXN_START_ID && ts > txn->GetReadTs()) {
    return true;
  }
  return false;
}

auto GenerateOrFindUndoLink(const Schema *schema, TransactionManager *txn_mgr, Transaction *txn,
                            const Tuple *base_tuple, timestamp_t original_ts, const Tuple *target_tuple,
                            std::optional<UndoLink> original_undo_link) -> std::optional<UndoLink> {
  std::optional<UndoLog> target_undo_log = std::nullopt;
  std::optional<UndoLink> target_undo_link = original_undo_link;
  if (target_undo_link.has_value()) {
    auto undo_log_opt = txn_mgr->GetUndoLogOptional(target_undo_link.value());
    if (undo_log_opt.has_value() && target_undo_link->prev_txn_ == txn->GetTransactionId()) {
      target_undo_log = undo_log_opt;
    }
  }
  if (target_undo_log.has_value()) {
    // update undo log
    auto undo_log = GenerateUpdatedUndoLog(schema, base_tuple, target_tuple, target_undo_log.value());
    txn->ModifyUndoLog(target_undo_link->prev_log_idx_, undo_log);
  } else if (original_ts != txn->GetTransactionTempTs()) {
    // insert new undo log if it is not the first
    auto prev_link = target_undo_link.has_value() ? target_undo_link.value() : UndoLink();
    auto undo_log = GenerateNewUndoLog(schema, base_tuple, target_tuple, original_ts, prev_link);
    target_undo_link = txn->AppendUndoLog(undo_log);
  }
  return target_undo_link;
}

auto GetUndoLogSchema(const Schema *schema, const UndoLog &log) -> Schema {
  std::vector<Column> columns;
  for (size_t i = 0; i < schema->GetColumnCount(); ++i) {
    if (log.modified_fields_[i]) {
      columns.push_back(schema->GetColumn(i));
    }
  }
  return Schema(columns);
}

auto CheckKeyIfExistInIndex(const Schema *table_schema, const Tuple *tuple, IndexInfo *index_info, Transaction *txn,
                            const TableInfo *table_info) -> std::pair<bool, RID> {
  std::vector<RID> rids;
  auto key_tuple = tuple->KeyFromTuple(*table_schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
  index_info->index_->ScanKey(key_tuple, &rids, txn);
  if (!rids.empty()) {
    auto rid = rids[0];
    auto tuple_meta = table_info->table_->GetTupleMeta(rid);
    if (IsWriteWriteConflict(txn, tuple_meta.ts_)) {
      return {true, rid};
    }
    if (!tuple_meta.is_deleted_) {
      return {true, rid};
    }
    return {false, rid};
  }
  return {false, RID()};
}

void InsertTupleAndIndexKey(const Tuple *new_tuple, TransactionManager *txn_mgr, Transaction *txn,
                            const TableInfo *table_info, std::optional<RID> &rid_opt, LockManager *lock_mgr,
                            std::vector<std::shared_ptr<IndexInfo>> &indexes) {
  if (rid_opt.has_value()) {
    // update for delete marked tuple
    auto tuple_link_info = GetTupleAndUndoLink(txn_mgr, table_info->table_.get(), rid_opt.value());
    auto &tuple_meta = std::get<0>(tuple_link_info);
    auto &old_tuple = std::get<1>(tuple_link_info);
    auto cur_ts = tuple_meta.ts_;
    if (IsWriteWriteConflict(txn, cur_ts)) {
      txn->SetTainted();
      throw ExecutionException("w-w conflict");
    }
    auto tuple_ptr = &old_tuple;
    if (tuple_meta.is_deleted_) {
      tuple_ptr = nullptr;
    }
    auto target_undo_link = GenerateOrFindUndoLink(&table_info->schema_, txn_mgr, txn, tuple_ptr, tuple_meta.ts_,
                                                   new_tuple, std::get<2>(tuple_link_info));
    auto check = [cur_ts](const TupleMeta &meta, const Tuple &tuple, RID rid, std::optional<UndoLink> undo_link) {
      return meta.ts_ == cur_ts;
    };
    tuple_meta.is_deleted_ = false;
    tuple_meta.ts_ = txn->GetTransactionTempTs();
    auto updated = UpdateTupleAndUndoLink(txn_mgr, rid_opt.value(), target_undo_link, table_info->table_.get(), txn,
                                          tuple_meta, *new_tuple, check);
    if (!updated) {
      txn->SetTainted();
      throw ExecutionException("Update failed");
    }
  } else {
    // insert new tuple
    auto tuple_meta = TupleMeta{txn->GetTransactionTempTs(), false};
    rid_opt = table_info->table_->InsertTuple(tuple_meta, *new_tuple, lock_mgr, txn, table_info->oid_);
    for (const auto &index : indexes) {
      // only consider primary key, for other index, mvcc will need logical delete on index
      if (!index->is_primary_key_) {
        continue;
      }
      auto inserted = index->index_->InsertEntry(
          new_tuple->KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
          rid_opt.value(), txn);
      if (!inserted) {
        txn->SetTainted();
        throw ExecutionException("Duplicate key");
      }
    }
  }
}

}  // namespace bustub
