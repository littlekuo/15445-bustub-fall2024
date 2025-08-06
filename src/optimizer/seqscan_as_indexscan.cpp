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
#include "optimizer/optimizer_internal.h"

namespace bustub {

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
                                                   seq_scan_plan.filter_predicate_);
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
      return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, table_info->oid_,
                                                 best_match_result.index_->index_oid_, seq_scan_plan.filter_predicate_);
    }
  }

  return optimized_plan;
}

}  // namespace bustub
