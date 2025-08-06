//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// optimizer_internal.h
//
// Identification: src/include/optimizer/optimizer_internal.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>
#include "catalog/catalog.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"

namespace bustub {

// Note: You can define your optimizer helper functions here
void OptimizerHelperFunction();

enum class PushdownTarget { LEFT, RIGHT, NONE };

struct IndexCondition {
  // Equal, GreaterThan, GreaterThanOrEqual
  ComparisonType type_;
  AbstractExpressionRef column_;
  AbstractExpressionRef constant_value_;
};

struct IndexMatchResult {
  bool is_valid_{false};
  const IndexInfo *index_{nullptr};
  std::vector<IndexCondition> index_conditions_;
  std::vector<AbstractExpressionRef> remaining_conditions_;
  uint32_t equality_condition_count_{0};
  uint32_t range_condition_count_{0};
};

auto SwapComparisonType(ComparisonType type) -> ComparisonType;

auto CompareIndexResult(const IndexMatchResult &a, const IndexMatchResult &b) -> bool;

void DecomposeConjunction(const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> &predicates);

auto BuildValidComparisonOnColumn(const AbstractExpressionRef &expr, const uint32_t &target_column_idx,
                                  const std::string &target_column_name, IndexCondition &condition) -> bool;

void MatchIndexWithPreds(const std::vector<AbstractExpressionRef> &predicates, const IndexInfo *index,
                         IndexMatchResult &result);
auto ExtractFullPrefixMatch(const std::vector<AbstractExpressionRef> &predicates, const IndexInfo *index)
    -> IndexMatchResult;

auto DecomposeEquiJoin(const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> &left,
                       std::vector<AbstractExpressionRef> &right) -> bool;

// parse conjunction, find full prefix match to support point lookup
auto FindDisjunctiveIndexConditions(const AbstractExpressionRef &expr, const IndexInfo *index,
                                    std::vector<std::vector<AbstractExpressionRef>> &point_lookups) -> bool;

// current only consider column and comparison expression
auto AnalyzePredicateForPushdown(const AbstractExpressionRef &expr) -> PushdownTarget;

auto RewritePredicate(const AbstractExpressionRef &original_pred, const AbstractPlanNodeRef &plan)
    -> std::unique_ptr<AbstractExpression>;

}  // namespace bustub
