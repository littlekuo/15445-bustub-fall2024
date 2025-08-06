//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// column_pruning.cpp
//
// Identification: src/optimizer/column_pruning.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <set>
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/mock_scan_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto PackTupleAndColumnIdx(uint32_t tuple_idx, uint32_t col_idx) -> uint64_t {
  return (static_cast<uint64_t>(tuple_idx) << 32) | col_idx;
}

auto UnpackTupleAndColumnIdx(uint64_t packed) -> std::pair<uint32_t, uint32_t> {
  return {static_cast<uint32_t>(packed >> 32), static_cast<uint32_t>(packed & 0xFFFFFFFF)};
}

inline auto IsScanPlan(const AbstractPlanNodeRef &plan) -> bool {
  return plan->GetType() == PlanType::SeqScan || plan->GetType() == PlanType::MockScan ||
         plan->GetType() == PlanType::IndexScan;
}

auto ConstructUpdatedColIdxMap(std::vector<uint64_t> &packed) -> std::unordered_map<uint64_t, uint64_t> {
  RemoveDuplicate(packed);
  std::unordered_map<uint32_t, uint32_t> used_col_idx;
  std::unordered_map<uint64_t, uint64_t> col_idx_map;
  for (auto &info : packed) {
    auto [tuple_idx, col_idx] = UnpackTupleAndColumnIdx(info);
    if (used_col_idx.find(tuple_idx) == used_col_idx.end()) {
      used_col_idx[tuple_idx] = 0;
    } else {
      used_col_idx[tuple_idx]++;
    }
    col_idx_map[info] = PackTupleAndColumnIdx(tuple_idx, used_col_idx[tuple_idx]);
  }
  return col_idx_map;
}

/**
 * @brief column pruning for child plan following a projection plan
 * @param plan the plan to optimize
 * @return the new plan with column pruning
 * @note You may use this function to implement column pruning optimization.
 */
auto Optimizer::OptimizeColumnPruning(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  if (plan->GetType() == PlanType::Insert || plan->GetType() == PlanType::Delete ||
      plan->GetType() == PlanType::Update) {
    return plan;
  }
  std::vector<uint32_t> required_columns;
  for (size_t i = 0; i < plan->OutputSchema().GetColumnCount(); i++) {
    required_columns.push_back(i);
  }
  return OptimizeColumnPruning(plan, required_columns);
}

auto Optimizer::OptimizeColumnPruning(const AbstractPlanNodeRef &plan, const std::vector<uint32_t> &required_columns)
    -> AbstractPlanNodeRef {
  if (plan->GetType() == PlanType::Projection) {
    BUSTUB_ASSERT(plan->GetChildren().size() == 1, "Projection plan must have exactly one child");
    auto proj_plan = std::dynamic_pointer_cast<const ProjectionPlanNode>(plan);
    std::vector<AbstractExpressionRef> needed_exprs;
    if (required_columns.empty()) {
      // as the start point
      needed_exprs = proj_plan->GetExpressions();
    } else {
      // prune from the parent
      for (auto col_idx : required_columns) {
        needed_exprs.push_back(proj_plan->GetExpressions()[col_idx]);
      }
    }
    std::vector<uint64_t> old_column_infos;
    for (auto &expr : needed_exprs) {
      ExtractOldColumns(expr, old_column_infos);
    }
    auto col_idx_map = ConstructUpdatedColIdxMap(old_column_infos);
    std::vector<AbstractExpressionRef> new_exprs;
    new_exprs.reserve(needed_exprs.size());
    for (auto &expr : needed_exprs) {
      new_exprs.push_back(RewriteExpressionForColumnOrder(expr, col_idx_map));
    }
    std::vector<uint32_t> child_required_columns;
    child_required_columns.reserve(old_column_infos.size());
    for (auto info : old_column_infos) {
      child_required_columns.push_back(UnpackTupleAndColumnIdx(info).second);
    }
    auto child_plan = OptimizeColumnPruning(proj_plan->GetChildPlan(), child_required_columns);
    SchemaRef new_schema = std::make_shared<Schema>(proj_plan->InferProjectionSchema(new_exprs));
    return std::make_shared<ProjectionPlanNode>(new_schema, new_exprs, child_plan);
  }

  std::vector<AbstractExpressionRef> new_exprs;
  new_exprs.reserve(required_columns.size());
  AbstractPlanNodeRef new_child_plan;

  if (plan->GetType() == PlanType::SeqScan) {
    BUSTUB_ASSERT(plan->GetChildren().empty(), "SeqScan plan must have no children");
    auto seq_scan_plan = std::dynamic_pointer_cast<const SeqScanPlanNode>(plan);
    for (auto col_idx : required_columns) {
      new_exprs.push_back(
          std::make_shared<ColumnValueExpression>(0, col_idx, seq_scan_plan->OutputSchema().GetColumn(col_idx)));
    }
    new_child_plan = seq_scan_plan->CloneWithChildren({});
  } else if (plan->GetType() == PlanType::MockScan) {
    BUSTUB_ASSERT(plan->GetChildren().empty(), "MockScan plan must have no children");
    auto mock_scan_plan = std::dynamic_pointer_cast<const MockScanPlanNode>(plan);
    for (auto col_idx : required_columns) {
      new_exprs.push_back(
          std::make_shared<ColumnValueExpression>(0, col_idx, mock_scan_plan->OutputSchema().GetColumn(col_idx)));
    }
    new_child_plan = mock_scan_plan->CloneWithChildren({});
  } else if (plan->GetType() == PlanType::IndexScan) {
    BUSTUB_ASSERT(plan->GetChildren().empty(), "IndexScan plan must have no children");
    auto index_scan_plan = std::dynamic_pointer_cast<const IndexScanPlanNode>(plan);
    for (auto col_idx : required_columns) {
      new_exprs.push_back(
          std::make_shared<ColumnValueExpression>(0, col_idx, index_scan_plan->OutputSchema().GetColumn(col_idx)));
    }
    new_child_plan = index_scan_plan->CloneWithChildren({});
  } else if (plan->GetType() == PlanType::NestedLoopJoin) {
    BUSTUB_ASSERT(plan->GetChildren().size() == 2, "NestedLoopJoin plan must have exactly two children");
    auto nlj_plan = std::dynamic_pointer_cast<const NestedLoopJoinPlanNode>(plan);

    std::vector<AbstractExpressionRef> needed_exprs;
    std::vector<uint64_t> old_column_infos;
    for (auto col_idx : required_columns) {
      if (col_idx < nlj_plan->GetLeftPlan()->OutputSchema().GetColumnCount()) {
        needed_exprs.push_back(
            std::make_shared<ColumnValueExpression>(0, col_idx, plan->OutputSchema().GetColumn(col_idx)));
        old_column_infos.push_back(PackTupleAndColumnIdx(0, col_idx));
      } else {
        needed_exprs.push_back(std::make_shared<ColumnValueExpression>(
            1, col_idx - nlj_plan->GetLeftPlan()->OutputSchema().GetColumnCount(),
            plan->OutputSchema().GetColumn(col_idx)));
        old_column_infos.push_back(
            PackTupleAndColumnIdx(1, col_idx - nlj_plan->GetLeftPlan()->OutputSchema().GetColumnCount()));
      }
    }
    ExtractOldColumns(nlj_plan->predicate_, old_column_infos);
    auto col_idx_map = ConstructUpdatedColIdxMap(old_column_infos);

    // rewrite predicate
    AbstractExpressionRef new_predicate = RewriteExpressionForColumnOrder(nlj_plan->predicate_, col_idx_map);

    std::vector<uint32_t> left_required_columns;
    std::vector<uint32_t> right_required_columns;
    for (auto info : old_column_infos) {
      auto col = UnpackTupleAndColumnIdx(info);
      if (col.first == 0) {
        left_required_columns.push_back(col.second);
      } else {
        right_required_columns.push_back(col.second);
      }
    }

    auto left_plan = OptimizeColumnPruning(nlj_plan->GetLeftPlan(), left_required_columns);
    auto right_plan = OptimizeColumnPruning(nlj_plan->GetRightPlan(), right_required_columns);
    SchemaRef new_schema = std::make_shared<Schema>(NestedLoopJoinPlanNode::InferJoinSchema(*left_plan, *right_plan));
    auto new_nlj_plan = std::make_shared<NestedLoopJoinPlanNode>(new_schema, left_plan, right_plan, new_predicate,
                                                                 nlj_plan->GetJoinType());

    for (auto &expr : needed_exprs) {
      auto new_expr = RewriteExpressionForColumnOrder(expr, col_idx_map);
      const auto *col_expr = dynamic_cast<const ColumnValueExpression *>(new_expr.get());
      if (col_expr->GetTupleIdx() == 0) {
        new_exprs.push_back(new_expr);
        continue;
      }
      new_exprs.push_back(std::make_shared<ColumnValueExpression>(
          0, col_expr->GetColIdx() + left_required_columns.size(), col_expr->GetReturnType()));
    }

    if (new_exprs.size() == new_schema->GetColumnCount()) {
      return new_nlj_plan;
    }
    new_child_plan = new_nlj_plan;
  } else if (plan->GetType() == PlanType::Filter) {
    BUSTUB_ASSERT(plan->GetChildren().size() == 1, "Filter plan must have exactly one child");
    auto filter_plan = std::dynamic_pointer_cast<const FilterPlanNode>(plan);

    std::vector<uint64_t> old_column_infos;
    old_column_infos.reserve(required_columns.size());
    for (auto col_idx : required_columns) {
      old_column_infos.push_back(PackTupleAndColumnIdx(0, col_idx));
    }
    ExtractOldColumns(filter_plan->predicate_, old_column_infos);
    auto col_idx_map = ConstructUpdatedColIdxMap(old_column_infos);

    AbstractExpressionRef new_predicate = RewriteExpressionForColumnOrder(filter_plan->predicate_, col_idx_map);
    std::vector<uint32_t> child_required_columns;
    child_required_columns.reserve(required_columns.size());
    for (auto info : old_column_infos) {
      child_required_columns.push_back(UnpackTupleAndColumnIdx(info).second);
    }
    auto child_plan = OptimizeColumnPruning(filter_plan->GetChildPlan(), child_required_columns);
    auto new_filter_plan = std::make_shared<FilterPlanNode>(std::make_shared<Schema>(child_plan->OutputSchema()),
                                                            new_predicate, child_plan);
    if (new_filter_plan->OutputSchema().GetColumnCount() == required_columns.size()) {
      return new_filter_plan;
    }
    for (auto col_idx : required_columns) {
      auto old_expr =
          std::make_shared<ColumnValueExpression>(0, col_idx, filter_plan->OutputSchema().GetColumn(col_idx));
      new_exprs.push_back(RewriteExpressionForColumnOrder(old_expr, col_idx_map));
    }
    new_child_plan = new_filter_plan;
  } else if (plan->GetType() == PlanType::Sort) {
    BUSTUB_ASSERT(plan->GetChildren().size() == 1, "Sort plan must have exactly one child");
    auto sort_plan = std::dynamic_pointer_cast<const SortPlanNode>(plan);

    std::vector<uint64_t> old_column_infos;
    old_column_infos.reserve(required_columns.size());
    for (auto col : required_columns) {
      old_column_infos.push_back(PackTupleAndColumnIdx(0, col));
    }
    for (auto &order_by : sort_plan->GetOrderBy()) {
      ExtractOldColumns(order_by.second, old_column_infos);
    }
    auto col_idx_map = ConstructUpdatedColIdxMap(old_column_infos);
    std::vector<OrderBy> new_order_by;
    for (auto &order_by : sort_plan->GetOrderBy()) {
      new_order_by.emplace_back(order_by.first, RewriteExpressionForColumnOrder(order_by.second, col_idx_map));
    }
    std::vector<uint32_t> child_required_columns;
    child_required_columns.reserve(old_column_infos.size());
    for (auto info : old_column_infos) {
      child_required_columns.push_back(UnpackTupleAndColumnIdx(info).second);
    }
    auto child_plan = OptimizeColumnPruning(sort_plan->GetChildPlan(), child_required_columns);
    auto new_sort_plan =
        std::make_shared<SortPlanNode>(std::make_shared<Schema>(child_plan->OutputSchema()), child_plan, new_order_by);
    if (new_sort_plan->OutputSchema().GetColumnCount() == required_columns.size()) {
      return new_sort_plan;
    }
    for (auto col_idx : required_columns) {
      auto old_expr = std::make_shared<ColumnValueExpression>(0, col_idx, sort_plan->OutputSchema().GetColumn(col_idx));
      new_exprs.push_back(RewriteExpressionForColumnOrder(old_expr, col_idx_map));
    }
    new_child_plan = new_sort_plan;
  } else if (plan->GetType() == PlanType::Aggregation) {
    BUSTUB_ASSERT(plan->GetChildren().size() == 1, "Aggregation plan must have exactly one child");
    auto agg_plan = std::dynamic_pointer_cast<const AggregationPlanNode>(plan);
    std::vector<uint64_t> old_column_infos;
    for (auto &expr : agg_plan->GetGroupBys()) {
      ExtractOldColumns(expr, old_column_infos);
    }
    // extract aggregates
    std::vector<AbstractExpressionRef> aggreagates;
    std::vector<AggregationType> aggregate_types;
    for (auto &i : required_columns) {
      if (i < agg_plan->GetGroupBys().size()) {
        continue;
      }
      aggreagates.push_back(agg_plan->GetAggregates()[i - agg_plan->GetGroupBys().size()]);
      aggregate_types.push_back(agg_plan->GetAggregateTypes()[i - agg_plan->GetGroupBys().size()]);
    }
    for (auto &expr : aggreagates) {
      ExtractOldColumns(expr, old_column_infos);
    }
    auto col_idx_map = ConstructUpdatedColIdxMap(old_column_infos);
    std::vector<AbstractExpressionRef> new_group_bys;
    std::vector<AbstractExpressionRef> new_aggregates;
    for (auto &expr : agg_plan->GetGroupBys()) {
      new_group_bys.push_back(RewriteExpressionForColumnOrder(expr, col_idx_map));
    }
    new_aggregates.reserve(aggreagates.size());
    for (auto &expr : aggreagates) {
      new_aggregates.push_back(RewriteExpressionForColumnOrder(expr, col_idx_map));
    }
    std::vector<uint32_t> child_required_columns;
    child_required_columns.reserve(old_column_infos.size());
    for (auto info : old_column_infos) {
      child_required_columns.push_back(UnpackTupleAndColumnIdx(info).second);
    }
    auto child_plan = OptimizeColumnPruning(agg_plan->GetChildPlan(), child_required_columns);
    auto new_schema =
        std::make_shared<Schema>(AggregationPlanNode::InferAggSchema(new_group_bys, new_aggregates, aggregate_types));
    auto new_agg_plan =
        std::make_shared<AggregationPlanNode>(new_schema, child_plan, new_group_bys, new_aggregates, aggregate_types);
    if (new_agg_plan->OutputSchema().GetColumnCount() == required_columns.size()) {
      return new_agg_plan;
    }
    auto idx = 0;
    for (auto col_idx : required_columns) {
      std::shared_ptr<AbstractExpression> old_expr;
      if (col_idx < agg_plan->GetGroupBys().size()) {
        old_expr = std::make_shared<ColumnValueExpression>(0, col_idx, agg_plan->OutputSchema().GetColumn(col_idx));
      } else {
        old_expr = std::make_shared<ColumnValueExpression>(0, agg_plan->GetGroupBys().size() + idx,
                                                           agg_plan->OutputSchema().GetColumn(col_idx));
        idx++;
      }
      new_exprs.push_back(old_expr);
    }
    new_child_plan = new_agg_plan;
  } else {
    BUSTUB_ASSERT(plan->GetChildren().size() == 1, "Only support at most one child plan");
    std::vector<AbstractPlanNodeRef> new_children;
    for (auto &child : plan->GetChildren()) {
      std::vector<uint32_t> new_required_columns;
      for (size_t i = 0; i < child->OutputSchema().GetColumnCount(); i++) {
        new_required_columns.push_back(i);
      }
      new_children.push_back(OptimizeColumnPruning(child, new_required_columns));
    }
    std::shared_ptr<AbstractPlanNode> new_plan = plan->CloneWithChildren(new_children);
    if (required_columns.size() == plan->OutputSchema().GetColumnCount()) {
      return new_plan;
    }
    for (auto col_idx : required_columns) {
      auto old_expr = std::make_shared<ColumnValueExpression>(0, col_idx, plan->OutputSchema().GetColumn(col_idx));
      new_exprs.push_back(old_expr);
    }
    new_child_plan = new_plan;
  }
  return std::make_shared<ProjectionPlanNode>(
      std::make_shared<Schema>(ProjectionPlanNode::InferProjectionSchema(new_exprs)), new_exprs, new_child_plan);
}

void Optimizer::ExtractOldColumns(const AbstractExpressionRef &expr, std::vector<uint64_t> &old_columns) {
  if (const auto *col_expr = dynamic_cast<const ColumnValueExpression *>(expr.get()); col_expr != nullptr) {
    auto packed_index = PackTupleAndColumnIdx(col_expr->GetTupleIdx(), col_expr->GetColIdx());
    old_columns.push_back(packed_index);
  }
  for (const auto &child : expr->children_) {
    ExtractOldColumns(child, old_columns);
  }
}

auto Optimizer::RewriteExpressionForColumnOrder(const AbstractExpressionRef &expr,
                                                const std::unordered_map<uint64_t, uint64_t> &col_idx_map,
                                                bool need_assert) -> AbstractExpressionRef {
  if (const auto *col_expr = dynamic_cast<const ColumnValueExpression *>(expr.get()); col_expr != nullptr) {
    auto packed_index = PackTupleAndColumnIdx(col_expr->GetTupleIdx(), col_expr->GetColIdx());
    if (need_assert) {
      BUSTUB_ASSERT(col_idx_map.find(packed_index) != col_idx_map.end(), "Column index not found in map");
    }
    auto new_packed_idx = col_idx_map.at(packed_index);
    auto new_unpacked_info = UnpackTupleAndColumnIdx(new_packed_idx);
    return std::make_shared<ColumnValueExpression>(col_expr->GetTupleIdx(), new_unpacked_info.second,
                                                   col_expr->GetReturnType());
  }
  std::vector<AbstractExpressionRef> new_children;
  for (const auto &child : expr->children_) {
    new_children.push_back(RewriteExpressionForColumnOrder(child, col_idx_map));
  }
  return expr->CloneWithChildren(new_children);
}

}  // namespace bustub
