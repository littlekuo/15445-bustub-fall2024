//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nlj_as_hash_join.cpp
//
// Identification: src/optimizer/nlj_as_hash_join.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

void DecomposeConjunc(const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> &predicates) {
  const auto *logic_expr = dynamic_cast<LogicExpression *>(expr.get());
  if (logic_expr != nullptr && logic_expr->logic_type_ == LogicType::And) {
    DecomposeConjunc(expr->GetChildAt(0), predicates);
    DecomposeConjunc(expr->GetChildAt(1), predicates);
  } else {
    predicates.push_back(expr);
  }
}

auto DecomposeEquiJoin(const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> &left,
                       std::vector<AbstractExpressionRef> &right) -> bool {
  const auto *comp_expr = dynamic_cast<ComparisonExpression *>(expr.get());
  if (comp_expr == nullptr) {
    return false;
  }
  if (comp_expr->comp_type_ != ComparisonType::Equal) {
    return false;
  }
  const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
  if (left_expr == nullptr) {
    return false;
  }
  const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
  if (right_expr == nullptr) {
    return false;
  }
  if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
    left.push_back(expr->children_[0]);
    right.push_back(expr->children_[1]);
    return true;
  }
  if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
    left.push_back(expr->children_[1]);
    right.push_back(expr->children_[0]);
    return true;
  }
  return false;
}

/**
 * @brief optimize nested loop join into hash join.
 * In the starter code, we will check NLJs with exactly one equal condition. You can further support optimizing joins
 * with multiple eq conditions.
 */
auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-conditions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    if (nlj_plan.Predicate() == nullptr) {
      return optimized_plan;
    }
    std::vector<AbstractExpressionRef> predicates;
    DecomposeConjunc(nlj_plan.Predicate(), predicates);
    std::vector<AbstractExpressionRef> left;
    std::vector<AbstractExpressionRef> right;
    bool is_equi_join = true;
    for (auto &predicate : predicates) {
      is_equi_join = DecomposeEquiJoin(predicate, left, right);
      if (!is_equi_join) {
        break;
      }
    }
    if (is_equi_join) {
      return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                nlj_plan.GetRightPlan(), left, right, nlj_plan.GetJoinType());
    }
  }
  return optimized_plan;
}

}  // namespace bustub
