//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "optimizer/optimizer.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new NestedLoopJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The nested loop join plan to be executed
 * @param left_executor The child executor that produces tuple for the left side of join
 * @param right_executor The child executor that produces tuple for the right side of join
 */
NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

/** Initialize the join */
void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  left_tuple_valid_ = false;
  left_tuple_matched_ = false;
}

/**
 * Yield the next tuple from the join.
 * @param[out] tuple The next tuple produced by the join
 * @param[out] rid The next tuple RID produced, not used by nested loop join.
 * @return `true` if a tuple was produced, `false` if there are no more tuples.
 */
auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (IsPredicateFalse(plan_->Predicate())) {
    return false;
  }
  while (true) {
    if (!left_tuple_valid_) {
      if (!left_executor_->Next(&left_tuple_, rid)) {
        return false;
      }
      left_tuple_valid_ = true;
      left_tuple_matched_ = false;
      right_executor_->Init();
    }
    Tuple right_tuple;
    bool right_exist = right_executor_->Next(&right_tuple, rid);
    if (right_exist) {
      auto val = plan_->Predicate()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                  right_executor_->GetOutputSchema());
      if (!val.IsNull() && val.GetAs<bool>()) {
        left_tuple_matched_ = true;
        std::vector<Value> values;
        for (size_t idx = 0; idx < plan_->GetLeftPlan()->OutputSchema().GetColumns().size(); idx++) {
          values.push_back(left_tuple_.GetValue(&plan_->GetLeftPlan()->OutputSchema(), idx));
        }
        for (size_t idx = 0; idx < plan_->GetRightPlan()->OutputSchema().GetColumns().size(); idx++) {
          values.push_back(right_tuple.GetValue(&plan_->GetRightPlan()->OutputSchema(), idx));
        }
        *tuple = Tuple(values, &plan_->OutputSchema());
        return true;
      }
    } else {
      left_tuple_valid_ = false;
      if (plan_->GetJoinType() == JoinType::LEFT && !left_tuple_matched_) {
        std::vector<Value> values;
        for (size_t idx = 0; idx < plan_->GetLeftPlan()->OutputSchema().GetColumns().size(); idx++) {
          values.push_back(left_tuple_.GetValue(&plan_->GetLeftPlan()->OutputSchema(), idx));
        }
        for (size_t idx = 0; idx < plan_->GetRightPlan()->OutputSchema().GetColumns().size(); idx++) {
          values.push_back(
              ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(idx).GetType()));
        }
        *tuple = Tuple(values, &plan_->OutputSchema());
        return true;
      }
    }
  }
  return false;
}

}  // namespace bustub
