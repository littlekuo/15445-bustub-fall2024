//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// optimizer_custom_rules.cpp
//
// Identification: src/optimizer/optimizer_custom_rules.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <set>
#include "execution/expressions/arithmetic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"
#include "optimizer/optimizer_internal.h"

// Note for 2023 Fall: You can add all optimizer rule implementations and apply the rules as you want in this file.
// Note that for some test cases, we force using starter rules, so that the configuration here won't take effects.
// Starter rule can be forcibly enabled by `set force_optimizer_starter_rule=yes`.

namespace bustub {

void RemoveDuplicate(std::vector<uint64_t> &packed) {
  std::sort(packed.begin(), packed.end());
  std::set<uint64_t> seen;
  packed.erase(std::remove_if(packed.begin(), packed.end(), [&seen](uint64_t x) { return !seen.insert(x).second; }),
               packed.end());
}

auto IsAllLeftChild(std::vector<uint64_t> &packed, uint32_t left_col_count) -> bool {
  return std::all_of(packed.begin(), packed.end(), [left_col_count](uint64_t x) {
    auto [tuple_idx, col_idx] = UnpackTupleAndColumnIdx(x);
    return col_idx < left_col_count;
  });
}

auto IsAllRightChild(std::vector<uint64_t> &packed, uint32_t left_col_count) -> bool {
  return std::all_of(packed.begin(), packed.end(), [left_col_count](uint64_t x) {
    auto [tuple_idx, col_idx] = UnpackTupleAndColumnIdx(x);
    return col_idx >= left_col_count;
  });
}

auto RewriteExpression(const AbstractExpressionRef &expr, uint32_t left_col_count) -> AbstractExpressionRef {
  if (const auto *col_expr = dynamic_cast<const ColumnValueExpression *>(expr.get()); col_expr != nullptr) {
    auto col_idx = col_expr->GetColIdx();
    BUSTUB_ASSERT(col_idx >= left_col_count, "Column index should be greater than or equal to left column count");
    return std::make_shared<ColumnValueExpression>(1, col_idx - left_col_count, col_expr->GetReturnType());
  }
  std::vector<AbstractExpressionRef> new_children;
  for (const auto &child : expr->children_) {
    new_children.push_back(RewriteExpression(child, left_col_count));
  }
  return expr->CloneWithChildren(new_children);
}

auto Optimizer::OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto p = plan;
  p = OptimizeMergeProjection(p);
  p = OptimizeMergeFilterNLJ(p);
  p = OptimizeMergeFilterScan(p);
  p = OptimizeColumnPruning(p);
  p = OptimizeConstantFolding(p);
  p = OptimizeMergeProjection(p);
  p = OptimizeMergeFilterNLJ(p);
  std::vector<AbstractExpressionRef> from_top;
  p = OptimizeDistributeNLJPredicates(p, from_top);
  p = OptimizeNLJAsHashJoin(p);
  p = OptimizeOrderByAsIndexScan(p);
  p = OptimizeSortLimitAsTopN(p);
  p = OptimizeMergeFilterScan(p);
  p = OptimizeSeqScanAsIndexScan(p);
  return p;
}

// not perfect but works
auto Optimizer::OptimizeDistributeNLJPredicates(const AbstractPlanNodeRef &plan,
                                                std::vector<AbstractExpressionRef> &from_top) -> AbstractPlanNodeRef {
  if (plan->GetType() == PlanType::SeqScan) {
    BUSTUB_ASSERT(plan->GetChildren().empty(), "SeqScan should not have children");
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*plan);
    if (seq_scan_plan.filter_predicate_ != nullptr) {
      from_top.push_back(seq_scan_plan.filter_predicate_);
    }
    return std::make_shared<SeqScanPlanNode>(seq_scan_plan.output_schema_, seq_scan_plan.GetTableOid(),
                                             seq_scan_plan.table_name_, CombinePredicates(from_top));
  }

  if (plan->GetType() == PlanType::MockScan) {
    BUSTUB_ASSERT(plan->GetChildren().empty(), "MockScan should not have children");
    std::vector<AbstractPlanNodeRef> children;
    std::shared_ptr<AbstractPlanNode> optimized_plan = plan->CloneWithChildren(children);
    if (!from_top.empty()) {
      return std::make_shared<FilterPlanNode>(plan->output_schema_, CombinePredicates(from_top), optimized_plan);
    }
    return optimized_plan;
  }

  auto optimize_recursive = [&](const AbstractPlanNodeRef &plan) {
    std::vector<AbstractPlanNodeRef> children;
    for (const auto &child : plan->GetChildren()) {
      std::vector<AbstractExpressionRef> child_from_top;
      children.push_back(OptimizeDistributeNLJPredicates(child, child_from_top));
    }
    return plan->CloneWithChildren(children);
  };

  if (plan->GetType() != PlanType::NestedLoopJoin) {
    std::shared_ptr<AbstractPlanNode> optimized_plan = optimize_recursive(plan);
    if (!from_top.empty()) {
      return std::make_shared<FilterPlanNode>(plan->output_schema_, CombinePredicates(from_top), optimized_plan);
    }
    return optimized_plan;
  }

  BUSTUB_ASSERT(plan->GetChildren().size() == 2, "NestedLoopJoin should have two children");
  const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*plan);
  auto predicate = nlj_plan.Predicate();
  // 1. No predicate to pushdown
  if (predicate == nullptr && from_top.empty()) {
    return optimize_recursive(plan);
  }
  if (nlj_plan.Predicate() != nullptr) {
    DecomposeConjunction(nlj_plan.Predicate(), from_top);
  }
  std::vector<AbstractExpressionRef> left_pushdown_preds;
  std::vector<AbstractExpressionRef> right_pushdown_preds;
  std::vector<AbstractExpressionRef> join_remain_preds;

  for (auto &pred : from_top) {
    PushdownTarget analysis = AnalyzePredicateForPushdown(pred);
    if (analysis == PushdownTarget::LEFT) {
      left_pushdown_preds.push_back(pred);
    } else if (analysis == PushdownTarget::RIGHT) {
      right_pushdown_preds.push_back(pred);
    } else {
      join_remain_preds.push_back(pred);
    }
  }

  std::vector<AbstractExpressionRef> rewritten_left_preds;
  std::vector<AbstractExpressionRef> rewritten_right_preds;
  rewritten_left_preds.reserve(left_pushdown_preds.size());
  rewritten_right_preds.reserve(right_pushdown_preds.size());
  for (const auto &pred : left_pushdown_preds) {
    rewritten_left_preds.push_back(RewritePredicate(pred, plan->GetChildAt(0)));
  }
  for (const auto &pred : right_pushdown_preds) {
    rewritten_right_preds.push_back(RewritePredicate(pred, plan->GetChildAt(1)));
  }

  std::vector<AbstractPlanNodeRef> refs;
  refs.push_back(OptimizeDistributeNLJPredicates(plan->GetChildAt(0), rewritten_left_preds));
  refs.push_back(OptimizeDistributeNLJPredicates(plan->GetChildAt(1), rewritten_right_preds));

  return std::make_shared<NestedLoopJoinPlanNode>(nlj_plan.output_schema_, refs[0], refs[1],
                                                  CombinePredicates(join_remain_preds), nlj_plan.GetJoinType());
}

auto Optimizer::OptimizeConstantFolding(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  BUSTUB_ASSERT(plan != nullptr, "Plan should not be null");
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.push_back(OptimizeConstantFolding(child));
  }
  auto optimized_plan = plan->CloneWithChildren(children);
  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    return std::make_shared<FilterPlanNode>(filter_plan.output_schema_, ConstantFolding(filter_plan.GetPredicate()),
                                            filter_plan.GetChildPlan());
  }
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    return std::make_shared<NestedLoopJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                    nlj_plan.GetRightPlan(), ConstantFolding(nlj_plan.Predicate()),
                                                    nlj_plan.GetJoinType());
  }
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    return std::make_shared<SeqScanPlanNode>(seq_scan_plan.output_schema_, seq_scan_plan.GetTableOid(),
                                             seq_scan_plan.table_name_,
                                             ConstantFolding(seq_scan_plan.filter_predicate_));
  }
  if (optimized_plan->GetType() == PlanType::IndexScan) {
    const auto &index_scan_plan = dynamic_cast<const IndexScanPlanNode &>(*optimized_plan);
    return std::make_shared<IndexScanPlanNode>(index_scan_plan.output_schema_, index_scan_plan.table_oid_,
                                               index_scan_plan.GetIndexOid(),
                                               ConstantFolding(index_scan_plan.filter_predicate_));
  }
  return optimized_plan;
}

// Constant folding for some cases
auto Optimizer::ConstantFolding(const AbstractExpressionRef &expr) -> AbstractExpressionRef {
  if (expr == nullptr) {
    return nullptr;
  }

  if (expr->GetChildren().empty()) {
    return expr->CloneWithChildren({});
  }

  std::vector<AbstractExpressionRef> new_children;
  for (const auto &child : expr->GetChildren()) {
    new_children.push_back(ConstantFolding(child));
  }
  auto new_expr = expr->CloneWithChildren(new_children);

  bool all_children_are_constants = true;
  for (const auto &child : new_expr->GetChildren()) {
    if (dynamic_cast<const ConstantValueExpression *>(child.get()) == nullptr) {
      all_children_are_constants = false;
      break;
    }
  }

  if (!all_children_are_constants) {
    return new_expr;
  }

  Value result_value;
  if (const auto *arith_expr = dynamic_cast<const ArithmeticExpression *>(new_expr.get()); arith_expr != nullptr) {
    const auto &left_val = dynamic_cast<const ConstantValueExpression *>(new_expr->GetChildAt(0).get())->val_;
    const auto &right_val = dynamic_cast<const ConstantValueExpression *>(new_expr->GetChildAt(1).get())->val_;

    switch (arith_expr->compute_type_) {
      case ArithmeticType::Plus:
        result_value = left_val.Add(right_val);
        break;
      case ArithmeticType::Minus:
        result_value = left_val.Subtract(right_val);
        break;
    }
  } else if (const auto *comp_expr = dynamic_cast<const ComparisonExpression *>(new_expr.get()); comp_expr != nullptr) {
    const auto &left_val = dynamic_cast<const ConstantValueExpression *>(new_expr->GetChildAt(0).get())->val_;
    const auto &right_val = dynamic_cast<const ConstantValueExpression *>(new_expr->GetChildAt(1).get())->val_;

    switch (comp_expr->comp_type_) {
      case ComparisonType::Equal:
        result_value = ValueFactory::GetBooleanValue(left_val.CompareEquals(right_val) == CmpBool::CmpTrue);
        break;
      case ComparisonType::NotEqual:
        result_value = ValueFactory::GetBooleanValue(left_val.CompareNotEquals(right_val) == CmpBool::CmpTrue);
        break;
      case ComparisonType::LessThan:
        result_value = ValueFactory::GetBooleanValue(left_val.CompareLessThan(right_val) == CmpBool::CmpTrue);
        break;
      case ComparisonType::LessThanOrEqual:
        result_value = ValueFactory::GetBooleanValue(left_val.CompareLessThanEquals(right_val) == CmpBool::CmpTrue);
        break;
      case ComparisonType::GreaterThan:
        result_value = ValueFactory::GetBooleanValue(left_val.CompareGreaterThan(right_val) == CmpBool::CmpTrue);
        break;
      case ComparisonType::GreaterThanOrEqual:
        result_value = ValueFactory::GetBooleanValue(left_val.CompareGreaterThanEquals(right_val) == CmpBool::CmpTrue);
        break;
    }
  } else {
    return new_expr;
  }

  return std::make_shared<ConstantValueExpression>(result_value);
}

auto IsPredicateFalse(const AbstractExpressionRef &expr) -> bool {
  if (const auto *const_expr = dynamic_cast<const ConstantValueExpression *>(expr.get()); const_expr != nullptr) {
    return !const_expr->val_.CastAs(TypeId::BOOLEAN).GetAs<bool>();
  }
  return false;
}

}  // namespace bustub
