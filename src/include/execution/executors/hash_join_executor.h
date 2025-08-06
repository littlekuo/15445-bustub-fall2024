//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {
struct JoinKey {
  std::vector<Value> values_;

  auto operator==(const JoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.values_.size(); i++) {
      if (values_[i].CompareEquals(other.values_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
}  // namespace bustub

namespace std {

template <>
struct hash<bustub::JoinKey> {
  auto operator()(const bustub::JoinKey &join_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : join_key.values_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};
}  // namespace std

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

  auto MakeJoinKey(const Tuple *tuple, const std::vector<AbstractExpressionRef> &exprs, const Schema &schema)
      -> JoinKey {
    std::vector<Value> keys;
    keys.reserve(exprs.size());
    for (const auto &expr : exprs) {
      keys.emplace_back(expr->Evaluate(tuple, schema));
    }
    return {keys};
  }

  auto ConcentrateTuples(const Tuple *left_tuple, const Tuple *right_tuple) -> Tuple {
    std::vector<Value> values;
    for (size_t i = 0; i < plan_->GetLeftPlan()->OutputSchema().GetColumnCount(); i++) {
      values.emplace_back(left_tuple->GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
    }
    if (right_tuple != nullptr) {
      for (size_t i = 0; i < plan_->GetRightPlan()->OutputSchema().GetColumnCount(); i++) {
        values.emplace_back(right_tuple->GetValue(&plan_->GetRightPlan()->OutputSchema(), i));
      }
    } else {
      for (size_t i = 0; i < plan_->GetRightPlan()->OutputSchema().GetColumnCount(); i++) {
        values.emplace_back(
            ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(i).GetType()));
      }
    }
    return {values, &GetOutputSchema()};
  }

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;
  std::unordered_map<JoinKey, std::vector<Tuple>> right_ht_{};
  std::unordered_map<JoinKey, std::vector<Tuple>>::iterator right_iter_;
  Tuple left_tuple_;
  JoinKey left_key_;
  bool left_tuple_valid_{false};
  bool right_iter_set_{false};
  uint32_t right_idx_{0};
};

}  // namespace bustub
