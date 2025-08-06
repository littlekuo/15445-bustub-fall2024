//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// optimizer_internal.cpp
//
// Identification: src/optimizer/optimizer_internal.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "optimizer/optimizer_internal.h"
#include "execution/expressions/arithmetic_expression.h"
#include "optimizer/optimizer.h"

namespace bustub {

void OptimizerHelperFunction() {}

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
  BUSTUB_ASSERT(expr != nullptr, "Expression cannot be null");
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

auto FindDisjunctiveIndexConditions(const AbstractExpressionRef &expr, const IndexInfo *index,
                                    std::vector<std::vector<AbstractExpressionRef>> &point_lookups) -> bool {
  BUSTUB_ASSERT(expr != nullptr, "Expression cannot be null");
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

auto AnalyzePredicateForPushdown(const AbstractExpressionRef &expr) -> PushdownTarget {
  int target_tuple_idx = -1;  // -1: unknown, 0: left, 1: right, -2: hybrid

  std::function<void(const AbstractExpressionRef &)> visit = [&](const AbstractExpressionRef &expr) {
    if (expr == nullptr || target_tuple_idx == -2) {
      return;
    }
    if (const auto *col_expr = dynamic_cast<const ColumnValueExpression *>(expr.get())) {
      if (target_tuple_idx == -1) {
        target_tuple_idx = col_expr->GetTupleIdx();
      } else if (target_tuple_idx != static_cast<int>(col_expr->GetTupleIdx())) {
        target_tuple_idx = -2;
      }
    }
    for (const auto &child : expr->GetChildren()) {
      visit(child);
    }
  };

  visit(expr);

  if (target_tuple_idx == 0) {
    return PushdownTarget::LEFT;
  }
  if (target_tuple_idx == 1) {
    return PushdownTarget::RIGHT;
  }
  return PushdownTarget::NONE;
}

auto RewritePredicate(const AbstractExpressionRef &original_pred, const AbstractPlanNodeRef &plan)
    -> std::unique_ptr<AbstractExpression> {
  if (original_pred == nullptr) {
    return nullptr;
  }

  if (const auto *col_expr = dynamic_cast<const ColumnValueExpression *>(original_pred.get())) {
    auto col_idx = col_expr->GetColIdx();
    auto cur_idx = 0;
    for (auto &child : plan->GetChildren()) {
      if (col_idx < child->OutputSchema().GetColumnCount()) {
        return std::make_unique<ColumnValueExpression>(cur_idx, col_idx, col_expr->GetReturnType());
      }
      col_idx -= child->OutputSchema().GetColumnCount();
      cur_idx++;
    }
    return std::make_unique<ColumnValueExpression>(cur_idx, col_idx, col_expr->GetReturnType());
  }

  if (const auto *expr = dynamic_cast<const ConstantValueExpression *>(original_pred.get())) {
    return std::make_unique<ConstantValueExpression>(expr->val_);
  }

  std::vector<AbstractExpressionRef> new_children;
  for (auto &expression : original_pred->GetChildren()) {
    new_children.push_back(RewritePredicate(expression, plan));
  }
  return original_pred->CloneWithChildren(new_children);
}

auto Optimizer::CombinePredicates(const std::vector<AbstractExpressionRef> &predicates) -> AbstractExpressionRef {
  if (predicates.empty()) {
    return nullptr;
  }
  if (predicates.size() == 1) {
    return predicates[0];
  }
  AbstractExpressionRef combined_pred{nullptr};
  for (auto &predicate : predicates) {
    if (combined_pred == nullptr || IsPredicateTrue(combined_pred)) {
      combined_pred = predicate;
    } else if (IsPredicateTrue(predicate)) {
      continue;
    } else {
      combined_pred = std::make_shared<LogicExpression>(combined_pred, predicate, LogicType::And);
    }
  }
  return combined_pred;
}

}  // namespace bustub
