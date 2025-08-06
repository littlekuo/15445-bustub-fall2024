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

namespace bustub {

/**
 * Builds a generic key from a vector of index prefix expressions.
 * @param key the key to build
 * @param exprs the index prefix expressions (constant expressions by index column order)
 * @param index_info the index info
 * @param table_schema the table schema
 */
void BuildTupleFromIndexPrefixExprs(Tuple *tuple, const std::vector<AbstractExpressionRef> &exprs,
                                    const std::shared_ptr<IndexInfo> &index_info, const Schema &table_schema) {
  std::vector<Value> values;
  for (size_t table_col_idx = 0, index_key_attr_idx = 0; table_col_idx < table_schema.GetColumns().size();
       table_col_idx++) {
    if (index_key_attr_idx < exprs.size()) {
      auto index_col_idx = index_info->index_->GetKeyAttrs()[index_key_attr_idx];
      if (table_col_idx == index_col_idx) {
        values.push_back(exprs[index_key_attr_idx]->Evaluate(nullptr, table_schema));
        index_key_attr_idx++;
        continue;
      }
    }
    values.push_back(Type::GetMinValue(table_schema.GetColumn(table_col_idx).GetType()));
  }
  *tuple = Tuple{values, &table_schema};
}

void BuildTupeFromGenericKey(Tuple *tuple, const IntegerKeyType_BTree &key, Schema &table_schema) {
  std::vector<Value> values;
  values.reserve(table_schema.GetColumns().size());
  for (size_t i = 0; i < table_schema.GetColumns().size(); i++) {
    values.emplace_back(key.ToValue(&table_schema, i));
  }
  *tuple = Tuple{values, &table_schema};
}

/**
 * Creates a new index scan executor.
 * @param exec_ctx the executor context
 * @param plan the index scan plan to be executed
 */
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto table_schema = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->schema_;
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  if (index_info == nullptr) {
    throw bustub::Exception("Index not found");
  }
  BUSTUB_ASSERT(index_info->index_type_ == IndexType::BPlusTreeIndex, "IndexScan only supports B+ tree now.");
  index_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());

  // 1. point lookup
  if (!plan_->point_lookup_keys_.empty()) {
    is_point_lookup_ = true;
    point_lookup_tuples_.reserve(plan_->point_lookup_keys_.size());
    for (auto &constant_exprs : plan_->point_lookup_keys_) {
      Tuple tuple;
      BuildTupleFromIndexPrefixExprs(&tuple, constant_exprs, index_info, table_schema);
      point_lookup_tuples_.emplace_back(tuple);
    }
    return;
  }
  // 2. range lookup
  std::vector<AbstractExpressionRef> start_key_exprs;
  ComparisonType last_comparison_type;
  for (auto &cond : plan_->range_lookup_conds_) {
    last_comparison_type = cond.type_;
    prefix_preds_.push_back(std::make_shared<ComparisonExpression>(cond.column_, cond.constant_value_, cond.type_));
    start_key_exprs.push_back(cond.constant_value_);
  }
  BuildTupleFromIndexPrefixExprs(&start_tuple_, start_key_exprs, index_info, table_schema);
  std::cout << start_tuple_.ToString(&table_schema) << std::endl;
  IntegerKeyType_BTree start_key;
  start_key.SetFromKey(start_tuple_);
  index_iterator_ = index_->GetBeginIterator(start_key);
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

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
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
      if (point_lookup_idx_ >= point_lookup_tuples_.size()) {
        return false;
      }
      auto &cur_tuple = point_lookup_tuples_[point_lookup_idx_++];
      std::vector<RID> rid_ret;
      index_->ScanKey(cur_tuple, &rid_ret, exec_ctx_->GetTransaction());
      if (!rid_ret.empty() && select_func(cur_tuple, plan_->remaining_conds_)) {
        *tuple = cur_tuple;
        *rid = rid_ret[0];
        return true;
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
    Tuple cur_tuple;
    BuildTupeFromGenericKey(&cur_tuple, cur_key, schema);
    if (!select_func(cur_tuple, prefix_preds_)) {
      index_iterator_ = index_->GetEndIterator();
      return false;
    }
    if (!select_func(cur_tuple, plan_->remaining_conds_)) {
      ++index_iterator_;
      continue;
    }
    *tuple = cur_tuple;
    *rid = cur_rid;
    ++index_iterator_;
    break;
  }
  return true;
}

}  // namespace bustub
