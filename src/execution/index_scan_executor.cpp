//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/index_scan_executor.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "optimizer/optimizer.h"
#include "optimizer/optimizer_internal.h"

namespace bustub {

/**
 * Builds a generic key from a vector of index prefix expressions.
 * @param key the key to build
 * @param exprs the index prefix expressions (constant expressions by index column order)
 * @param index_info the index info
 * @param table_schema the table schema
 */
void BuildTupleFromIndexPrefixExprs(Tuple *tuple, const std::vector<AbstractExpressionRef> &exprs,
                                    const std::shared_ptr<IndexInfo> &index_info) {
  std::vector<Value> values;
  for (size_t i = 0; i < index_info->key_schema_.GetColumns().size(); i++) {
    if (i < exprs.size()) {
      values.push_back(exprs[i]->Evaluate(nullptr, index_info->key_schema_));
      continue;
    }
    values.push_back(Type::GetMinValue(index_info->key_schema_.GetColumn(i).GetType()));
  }
  *tuple = Tuple{values, &index_info->key_schema_};
}

/**
 * Creates a new index scan executor.
 * @param exec_ctx the executor context
 * @param plan the index scan plan to be executed
 */
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  is_point_lookup_ = false;
  point_lookup_partial_tuples_.clear();
  point_lookup_idx_ = 0;
  prefix_preds_.clear();
  remaining_conds_.clear();

  auto table_schema = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->schema_;
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  if (index_info == nullptr) {
    throw bustub::Exception("Index not found");
  }
  BUSTUB_ASSERT(index_info->index_type_ == IndexType::BPlusTreeIndex, "IndexScan only supports B+ tree now.");
  index_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());

  // 1. no filter predicate
  if (plan_->filter_predicate_ == nullptr) {
    index_iterator_ = index_->GetBeginIterator();
    AbstractExpressionRef dummy_pred = std::make_shared<ConstantValueExpression>(Value(BOOLEAN, 1));
    exec_ctx_->GetTransaction()->AppendScanPredicate(plan_->table_oid_, dummy_pred);
    return;
  }
  exec_ctx_->GetTransaction()->AppendScanPredicate(plan_->table_oid_, plan_->filter_predicate_);

  {
    // 2. point lookup without remaining conditions
    std::vector<std::vector<AbstractExpressionRef>> point_lookups;
    if (FindDisjunctiveIndexConditions(plan_->filter_predicate_, index_info.get(), point_lookups)) {
      is_point_lookup_ = true;
      point_lookup_partial_tuples_.reserve(point_lookups.size());
      for (auto &constant_exprs : point_lookups) {
        Tuple tuple;
        BuildTupleFromIndexPrefixExprs(&tuple, constant_exprs, index_info);
        point_lookup_partial_tuples_.emplace_back(tuple);
      }
      return;
    }

    // 3. point lookup with remaining conditions / range lookup with specific prefix
    std::vector<AbstractExpressionRef> predicates;
    DecomposeConjunction(plan_->filter_predicate_, predicates);
    IndexMatchResult match_result;
    MatchIndexWithPreds(predicates, index_info.get(), match_result);
    if (match_result.is_valid_) {
      for (auto &cond : match_result.remaining_conditions_) {
        remaining_conds_.push_back(cond);
      }
      // 3.1 point lookup
      if (match_result.equality_condition_count_ == index_info->index_->GetKeyAttrs().size()) {
        is_point_lookup_ = true;
        point_lookup_partial_tuples_.reserve(match_result.index_conditions_.size());
        for (auto &cond : match_result.index_conditions_) {
          Tuple tuple;
          BuildTupleFromIndexPrefixExprs(&tuple, {cond.constant_value_}, index_info);
          point_lookup_partial_tuples_.emplace_back(tuple);
        }
        return;
      }
      // 3.2 range lookup with specific prefix
      std::vector<AbstractExpressionRef> start_key_exprs;
      ComparisonType last_comparison_type;
      for (auto &cond : match_result.index_conditions_) {
        last_comparison_type = cond.type_;
        prefix_preds_.push_back(std::make_shared<ComparisonExpression>(cond.column_, cond.constant_value_, cond.type_));
        start_key_exprs.push_back(cond.constant_value_);
      }
      BuildTupleFromIndexPrefixExprs(&start_partial_tuple_, start_key_exprs, index_info);
      IntegerKeyType_BTree start_key;
      start_key.SetFromKey(start_partial_tuple_);
      index_iterator_ = index_->GetBeginIterator(start_key);
      // skip the included start key if the last comparison is greater than
      if (!index_iterator_.IsEnd() && last_comparison_type == ComparisonType::GreaterThan) {
        auto [cur_key, _] = *index_iterator_;
        std::vector<uint32_t> attrs;
        for (size_t i = 0; i < start_key_exprs.size(); i++) {
          attrs.push_back(index_->GetKeyAttrs()[i]);
        }
        auto key_schema = Schema::CopySchema(&table_schema, attrs);
        IntegerComparatorType_BTree comparator(&key_schema);
        if (comparator(start_key, cur_key) == 0) {
          ++index_iterator_;
        }
      }
    }
    return;
  }

  // 4. scan the whole index in by order
  index_iterator_ = index_->GetBeginIterator();
  remaining_conds_.push_back(plan_->filter_predicate_);
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (IsPredicateFalse(plan_->filter_predicate_)) {
    return false;
  }
  auto select_func = [this](const Tuple &cur_tuple, const std::vector<AbstractExpressionRef> &preds) {
    for (auto &pred : preds) {
      auto value = pred->Evaluate(&cur_tuple, plan_->OutputSchema());
      if (value.IsNull() || !value.GetAs<bool>()) {
        return false;
      }
    }
    return true;
  };

  // 1. point lookup
  if (is_point_lookup_) {
    while (true) {
      if (point_lookup_idx_ >= point_lookup_partial_tuples_.size()) {
        return false;
      }
      auto &cur_tuple = point_lookup_partial_tuples_[point_lookup_idx_++];
      std::vector<RID> rid_ret;
      index_->ScanKey(cur_tuple, &rid_ret, exec_ctx_->GetTransaction());
      if (!rid_ret.empty()) {
        auto [tuple_meta_, tuple_, undo_link] =
            GetTupleAndUndoLink(exec_ctx_->GetTransactionManager(), table_info_->table_.get(), rid_ret[0]);
        auto undo_logs = CollectUndoLogs(rid_ret[0], tuple_meta_, tuple_, undo_link, exec_ctx_->GetTransaction(),
                                         exec_ctx_->GetTransactionManager());
        if (!undo_logs.has_value()) {
          // means tuple is not exist at that time
          continue;
        }
        auto rebuild_tuple = ReconstructTuple(&GetOutputSchema(), tuple_, tuple_meta_, undo_logs.value());
        if (!rebuild_tuple.has_value()) {
          // means tuple is deleted
          continue;
        }
        if (select_func(rebuild_tuple.value(), remaining_conds_)) {
          rebuild_tuple->SetRid(rid_ret[0]);
          *tuple = rebuild_tuple.value();
          *rid = rid_ret[0];
          return true;
        }
      }
    }
    return false;
  }

  // 2. range lookup
  if (index_iterator_.IsEnd()) {
    return false;
  }
  auto schema = plan_->OutputSchema();
  while (true) {
    if (index_iterator_.IsEnd()) {
      return false;
    }
    auto [cur_key, cur_rid] = *index_iterator_;
    auto [tuple_meta, tuple_, undo_link] =
        GetTupleAndUndoLink(exec_ctx_->GetTransactionManager(), table_info_->table_.get(), cur_rid);
    auto undo_logs = CollectUndoLogs(cur_rid, tuple_meta, tuple_, undo_link, exec_ctx_->GetTransaction(),
                                     exec_ctx_->GetTransactionManager());
    if (!undo_logs.has_value()) {
      ++index_iterator_;
      continue;
    }
    auto rebuild_tuple = ReconstructTuple(&GetOutputSchema(), tuple_, tuple_meta, undo_logs.value());
    if (!rebuild_tuple.has_value()) {
      ++index_iterator_;
      continue;
    }
    if (!select_func(rebuild_tuple.value(), prefix_preds_)) {
      index_iterator_ = index_->GetEndIterator();
      return false;
    }
    if (!select_func(rebuild_tuple.value(), remaining_conds_)) {
      ++index_iterator_;
      continue;
    }
    rebuild_tuple->SetRid(cur_rid);
    *tuple = rebuild_tuple.value();
    *rid = cur_rid;
    ++index_iterator_;
    break;
  }
  return true;
}

}  // namespace bustub
