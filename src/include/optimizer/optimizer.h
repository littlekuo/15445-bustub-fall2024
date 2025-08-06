//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// optimizer.h
//
// Identification: src/include/optimizer/optimizer.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "concurrency/transaction.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"

namespace bustub {

auto IsPredicateFalse(const AbstractExpressionRef &expr) -> bool;

void RemoveDuplicate(std::vector<uint64_t> &packed);

auto PackTupleAndColumnIdx(uint32_t tuple_idx, uint32_t col_idx) -> uint64_t;

auto UnpackTupleAndColumnIdx(uint64_t packed) -> std::pair<uint32_t, uint32_t>;

auto ConstructUpdatedColIdxMap(std::vector<uint64_t> &packed) -> std::unordered_map<uint64_t, uint64_t>;

/**
 * The optimizer takes an `AbstractPlanNode` and outputs an optimized `AbstractPlanNode`.
 */
class Optimizer {
 public:
  explicit Optimizer(const Catalog &catalog, bool force_starter_rule)
      : catalog_(catalog), force_starter_rule_(force_starter_rule) {}

  auto Optimize(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

 private:
  auto OptimizeMergeProjection(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizeMergeFilterNLJ(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizeNLJAsIndexJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizeEliminateTrueFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizeMergeFilterScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto RewriteExpressionForJoin(const AbstractExpressionRef &expr, size_t left_column_cnt, size_t right_column_cnt)
      -> AbstractExpressionRef;

  auto IsPredicateTrue(const AbstractExpressionRef &expr) -> bool;

  auto OptimizeOrderByAsIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizeSeqScanAsIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto MatchIndex(const std::string &table_name, uint32_t index_key_idx)
      -> std::optional<std::tuple<index_oid_t, std::string>>;

  auto OptimizeColumnPruning(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto EstimatedCardinality(const std::string &table_name) -> std::optional<size_t>;

  auto OptimizeDistributeNLJPredicates(const AbstractPlanNodeRef &plan, std::vector<AbstractExpressionRef> &from_top)
      -> AbstractPlanNodeRef;

  auto CombinePredicates(const std::vector<AbstractExpressionRef> &predicates) -> AbstractExpressionRef;

  auto OptimizeColumnPruning(const AbstractPlanNodeRef &plan, const std::vector<uint32_t> &required_columns)
      -> AbstractPlanNodeRef;

  void ExtractOldColumns(const AbstractExpressionRef &expr, std::vector<uint64_t> &old_columns);

  auto RewriteExpressionForColumnOrder(const AbstractExpressionRef &expr,
                                       const std::unordered_map<uint64_t, uint64_t> &col_idx_map,
                                       bool need_assert = true) -> AbstractExpressionRef;
  auto ConstantFolding(const AbstractExpressionRef &expr) -> AbstractExpressionRef;

  auto OptimizeConstantFolding(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /** Catalog will be used during the planning process. USERS SHOULD ENSURE IT OUTLIVES
   * OPTIMIZER, otherwise it's a dangling reference.
   */
  const Catalog &catalog_;

  const bool force_starter_rule_;
};

}  // namespace bustub
