//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

/**
 * Construct a new HashJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The HashJoin join plan to be executed
 * @param left_child The child executor that produces tuples for the left side of join
 * @param right_child The child executor that produces tuples for the right side of join
 */
HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for Fall 2024: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

/** Initialize the join */
void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  right_ht_.clear();
  Tuple tuple;
  RID rid;
  while (right_executor_->Next(&tuple, &rid)) {
    auto key = MakeJoinKey(&tuple, plan_->RightJoinKeyExpressions(), right_executor_->GetOutputSchema());
    right_ht_[key].push_back(std::move(tuple));
  }
  left_tuple_valid_ = false;
  right_iter_set_ = false;
  right_idx_ = 0;
}

/**
 * Yield the next tuple from the join.
 * @param[out] tuple The next tuple produced by the join.
 * @param[out] rid The next tuple RID, not used by hash join.
 * @return `true` if a tuple was produced, `false` if there are no more tuples.
 */
auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (!left_tuple_valid_) {
      if (!left_executor_->Next(&left_tuple_, rid)) {
        return false;
      }
      left_tuple_valid_ = true;
      left_key_ = MakeJoinKey(&left_tuple_, plan_->LeftJoinKeyExpressions(), left_executor_->GetOutputSchema());
    }
    if (!right_iter_set_) {
      right_iter_ = right_ht_.find(left_key_);
      right_idx_ = 0;
      right_iter_set_ = true;
    }
    if (right_iter_ == right_ht_.end()) {
      left_tuple_valid_ = false;
      right_iter_set_ = false;
      if (plan_->GetJoinType() == JoinType::LEFT) {
        *tuple = ConcentrateTuples(&left_tuple_, nullptr);
        break;
      }
    } else {
      if (right_idx_ >= right_iter_->second.size()) {
        left_tuple_valid_ = false;
        right_iter_set_ = false;
        continue;
      }
      *tuple = ConcentrateTuples(&left_tuple_, &right_iter_->second[right_idx_++]);
      break;
    }
  }
  return true;
}

}  // namespace bustub
