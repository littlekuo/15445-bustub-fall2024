//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seqscan_as_indexscan.cpp
//
// Identification: src/optimizer/seqscan_as_indexscan.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "binder/tokens.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

struct IndexMatchResult {
  bool is_valid_{false};
  const IndexInfo *index_{nullptr};
  std::vector<IndexCondition> index_conditions_;
  std::vector<AbstractExpressionRef> remaining_conditions_;
  uint32_t equality_condition_count_{0};
  uint32_t range_condition_count_{0};
};

auto SwapComparisonType(ComparisonType type) -> ComparisonType {
  switch (type) {
    case ComparisonType::Equal:
      return ComparisonType::Equal;
    case ComparisonType::LessThan:
      return ComparisonType::GreaterThan;
    case ComparisonType::LessThanOrEqual:
      return ComparisonType::GreaterThanOrEqual;
    case ComparisonType::GreaterThan:
      return ComparisonType::LessThan;
    case ComparisonType::GreaterThanOrEqual:
      return ComparisonType::LessThanOrEqual;
    default:
      return ComparisonType::NotEqual;  // or some invalid type
  }
}

auto CompareIndexResult(const IndexMatchResult &a, const IndexMatchResult &b) -> bool {
  if (a.equality_condition_count_ != b.equality_condition_count_) {
    return a.equality_condition_count_ > b.equality_condition_count_;
  }

  return a.range_condition_count_ > b.range_condition_count_;
}

void DecomposeConjunction(const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> &predicates) {
  const auto *logic_expr = dynamic_cast<LogicExpression *>(expr.get());
  if (logic_expr != nullptr && logic_expr->logic_type_ == LogicType::And) {
    DecomposeConjunction(expr->GetChildAt(0), predicates);
    DecomposeConjunction(expr->GetChildAt(1), predicates);
  } else {
    predicates.push_back(expr);
  }
}

auto BuildValidComparisonOnColumn(const AbstractExpressionRef &expr, const uint32_t &target_column_idx,
                                  const std::string &target_column_name, IndexCondition &condition) -> bool {
  auto comp_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
  if (comp_expr == nullptr) {
    return false;
  }

  ComparisonType op_type = comp_expr->comp_type_;
  if (op_type == ComparisonType::NotEqual) {
    return false;
  }

  auto left_child = comp_expr->GetChildAt(0);
  auto right_child = comp_expr->GetChildAt(1);

  auto left_col = dynamic_cast<const ColumnValueExpression *>(left_child.get());
  auto right_const = dynamic_cast<const ConstantValueExpression *>(right_child.get());

  // case 1: support equal / greater than / greater than or equal
  if (left_col != nullptr && right_const != nullptr) {
    if ((op_type == ComparisonType::Equal || op_type == ComparisonType::GreaterThanOrEqual ||
         op_type == ComparisonType::GreaterThan) &&
        left_col->GetColIdx() == target_column_idx) {
      condition.type_ = op_type;
      condition.column_ = left_child;
      condition.constant_value_ = right_child;
      return true;
    }
  }

  auto left_const = dynamic_cast<const ConstantValueExpression *>(left_child.get());
  auto right_col = dynamic_cast<const ColumnValueExpression *>(right_child.get());

  // case 2: support equal / less than / less than or equal
  if (left_const != nullptr && right_col != nullptr) {
    if ((op_type == ComparisonType::Equal || op_type == ComparisonType::LessThanOrEqual ||
         op_type == ComparisonType::LessThan) &&
        right_col->GetColIdx() == target_column_idx) {
      condition.type_ = SwapComparisonType(op_type);
      condition.column_ = right_child;
      condition.constant_value_ = left_child;
      return true;
    }
  }

  return false;
}

void MatchIndexWithPreds(const std::vector<AbstractExpressionRef> &predicates, const IndexInfo *index,
                         IndexMatchResult &result) {
  result.index_ = index;
  std::vector<AbstractExpressionRef> remaining_predicates = predicates;

  for (uint32_t idx = 0; idx < index->index_->GetKeyAttrs().size(); ++idx) {
    bool found_match_for_this_column = false;

    IndexCondition best;
    std::vector<AbstractExpressionRef>::iterator best_it;

    for (auto it = remaining_predicates.begin(); it != remaining_predicates.end(); ++it) {
      auto expr = *it;
      IndexCondition cond;
      auto original_col_idx = index->index_->GetKeyAttrs()[idx];
      auto original_col_name = index->index_->GetKeySchema()->GetColumn(idx).GetName();
      if (BuildValidComparisonOnColumn(expr, original_col_idx, original_col_name, cond)) {
        if (!found_match_for_this_column) {
          best = cond;
          best_it = it;
          found_match_for_this_column = true;
        } else if (cond.type_ == ComparisonType::Equal) {
          best = cond;
          best_it = it;
        }
      }
    }

    if (!found_match_for_this_column) {
      break;
    }

    result.is_valid_ = true;
    result.index_conditions_.push_back(best);
    remaining_predicates.erase(best_it);
    if (best.type_ == ComparisonType::Equal) {
      result.equality_condition_count_++;
    } else {
      result.range_condition_count_++;
      goto matching_done;
    }
  }

matching_done:
  result.remaining_conditions_ = remaining_predicates;
}

auto ExtractFullPrefixMatch(const std::vector<AbstractExpressionRef> &predicates, const IndexInfo *index)
    -> IndexMatchResult {
  IndexMatchResult result;
  MatchIndexWithPreds(predicates, index, result);

  if (result.is_valid_ && result.equality_condition_count_ == index->index_->GetKeyAttrs().size()) {
    return result;
  }

  return IndexMatchResult{};
}

// parse conjunction, find full prefix match to support point lookup
auto FindDisjunctiveIndexConditions(const AbstractExpressionRef &expr, const IndexInfo *index,
                                    std::vector<std::vector<AbstractExpressionRef>> &point_lookups) -> bool {
  const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get());
  if (logic_expr != nullptr && logic_expr->logic_type_ == LogicType::Or) {
    bool left_ok = FindDisjunctiveIndexConditions(expr->GetChildAt(0), index, point_lookups);
    bool right_ok = FindDisjunctiveIndexConditions(expr->GetChildAt(1), index, point_lookups);
    return left_ok && right_ok;
  }

  std::vector<AbstractExpressionRef> predicates;
  DecomposeConjunction(expr, predicates);

  IndexMatchResult match = ExtractFullPrefixMatch(predicates, index);
  if (match.is_valid_ && match.remaining_conditions_.empty()) {
    std::vector<AbstractExpressionRef> keys;
    for (const auto &cond : match.index_conditions_) {
      keys.push_back(cond.constant_value_);
    }
    point_lookups.push_back(keys);
    return true;
  }

  return false;
}

/**
 * @brief optimize seq scan as index scan if there's an index on a table
 * @note Fall 2023 only: using hash index and only support point lookup
 */
auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    if (seq_scan_plan.filter_predicate_ == nullptr) {
      return optimized_plan;
    }
    const auto table_info = catalog_.GetTable(seq_scan_plan.GetTableOid());
    const auto indices = catalog_.GetTableIndexes(table_info->name_);

    // try to generate point lookup first
    for (const auto &index : indices) {
      std::vector<std::vector<AbstractExpressionRef>> point_lookups;
      if (FindDisjunctiveIndexConditions(seq_scan_plan.filter_predicate_, index.get(), point_lookups)) {
        return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, table_info->oid_, index->index_oid_,
                                                   seq_scan_plan.filter_predicate_, point_lookups);
      }
    }

    // try to decompose conjunction and match index
    std::vector<AbstractExpressionRef> predicates;
    DecomposeConjunction(seq_scan_plan.filter_predicate_, predicates);
    IndexMatchResult best_match_result;
    for (const auto &index : indices) {
      IndexMatchResult match_result;
      MatchIndexWithPreds(predicates, index.get(), match_result);
      if (match_result.is_valid_ && CompareIndexResult(match_result, best_match_result)) {
        best_match_result = match_result;
      }
    }
    if (best_match_result.is_valid_) {
      // point lookup first, then apply remaining conditions
      if (best_match_result.equality_condition_count_ == best_match_result.index_->index_->GetKeyAttrs().size()) {
        std::vector<std::vector<AbstractExpressionRef>> point_lookups;
        std::vector<AbstractExpressionRef> keys;
        for (const auto &cond : best_match_result.index_conditions_) {
          keys.push_back(cond.constant_value_);
        }
        point_lookups.push_back(keys);
        return std::make_shared<IndexScanPlanNode>(
            seq_scan_plan.output_schema_, table_info->oid_, best_match_result.index_->index_oid_,
            seq_scan_plan.filter_predicate_, point_lookups, std::vector<IndexCondition>{},
            best_match_result.remaining_conditions_);
      }
      // range lookup
      return std::make_shared<IndexScanPlanNode>(
          seq_scan_plan.output_schema_, table_info->oid_, best_match_result.index_->index_oid_,
          seq_scan_plan.filter_predicate_, std::vector<std::vector<AbstractExpressionRef>>{},
          best_match_result.index_conditions_, best_match_result.remaining_conditions_);
    }
  }

  return optimized_plan;
}

}  // namespace bustub
