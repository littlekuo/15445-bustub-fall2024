//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Creates a new nested index join executor.
 * @param exec_ctx the context that the nested index join should be performed in
 * @param plan the nested index join plan to be executed
 * @param child_executor the outer table
 */
NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  left_tuple_valid_ = false;
  inner_index_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  inner_table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (!left_tuple_valid_) {
      if (!child_executor_->Next(&left_tuple_, rid)) {
        return false;
      }
      left_tuple_valid_ = true;
    }
    auto value = plan_->KeyPredicate()->Evaluate(&left_tuple_, child_executor_->GetOutputSchema());
    std::vector<Value> values{value};
    auto key = Tuple(values, inner_index_->index_->GetKeySchema());
    std::vector<RID> result;
    inner_index_->index_->ScanKey(key, &result, exec_ctx_->GetTransaction());
    left_tuple_valid_ = false;
    if (result.empty()) {
      if (plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> values;
        for (size_t idx = 0; idx < child_executor_->GetOutputSchema().GetColumns().size(); idx++) {
          values.push_back(left_tuple_.GetValue(&child_executor_->GetOutputSchema(), idx));
        }
        for (size_t idx = 0; idx < plan_->inner_table_schema_->GetColumns().size(); idx++) {
          values.push_back(ValueFactory::GetNullValueByType(plan_->inner_table_schema_->GetColumn(idx).GetType()));
        }
        *tuple = Tuple(values, &plan_->OutputSchema());
        return true;
      }
    } else {
      for (auto &rid : result) {
        auto [_, inner_tuple] = inner_table_info_->table_->GetTuple(rid);
        std::vector<Value> values;
        for (size_t idx = 0; idx < child_executor_->GetOutputSchema().GetColumns().size(); idx++) {
          values.push_back(left_tuple_.GetValue(&child_executor_->GetOutputSchema(), idx));
        }
        for (size_t idx = 0; idx < plan_->inner_table_schema_->GetColumns().size(); idx++) {
          values.push_back(inner_tuple.GetValue(&*plan_->inner_table_schema_, idx));
        }
        *tuple = Tuple(values, &plan_->OutputSchema());
        return true;
      }
    }
  }
}

}  // namespace bustub
