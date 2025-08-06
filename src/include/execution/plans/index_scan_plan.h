//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_plan.h
//
// Identification: src/include/execution/plans/index_scan_plan.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "concurrency/transaction.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/plans/abstract_plan.h"
#include "type/value.h"

namespace bustub {

struct IndexCondition {
  // Equal, GreaterThan, GreaterThanOrEqual
  ComparisonType type_;
  AbstractExpressionRef column_;
  AbstractExpressionRef constant_value_;
};

/**
 * IndexScanPlanNode identifies a table that should be scanned with an optional predicate.
 */
class IndexScanPlanNode : public AbstractPlanNode {
 public:
  /**
   * Creates a new index scan plan node with filter predicate.
   * @param output The output format of this scan plan node
   * @param table_oid The identifier of table to be scanned
   * @param filter_predicate The predicate pushed down to index scan.
   * @param pred_key The key for point lookup
   */
  IndexScanPlanNode(SchemaRef output, table_oid_t table_oid, index_oid_t index_oid,
                    AbstractExpressionRef filter_predicate = nullptr,
                    std::vector<std::vector<AbstractExpressionRef>> point_lookup_keys = {},
                    std::vector<IndexCondition> range_lookup_conds = {},
                    std::vector<AbstractExpressionRef> remaining_conditions = {})
      : AbstractPlanNode(std::move(output), {}),
        table_oid_(table_oid),
        index_oid_(index_oid),
        filter_predicate_(std::move(filter_predicate)),
        point_lookup_keys_(std::move(point_lookup_keys)),
        range_lookup_conds_(std::move(range_lookup_conds)),
        remaining_conds_(std::move(remaining_conditions)) {}

  auto GetType() const -> PlanType override { return PlanType::IndexScan; }

  /** @return the identifier of the table that should be scanned */
  auto GetIndexOid() const -> index_oid_t { return index_oid_; }

  BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(IndexScanPlanNode);

  /** The table which the index is created on. */
  table_oid_t table_oid_;

  /** The index whose tuples should be scanned. */
  index_oid_t index_oid_;

  /** The predicate to filter in index scan.
   * For Fall 2023, after you implemented seqscan to indexscan optimizer rule,
   * we can use this predicate to do index point lookup
   */
  AbstractExpressionRef filter_predicate_;

  /**
   * The constant value keys to lookup.
   * For example when dealing "WHERE v = 1" we could store the constant value 1 here
   * support index on multiple columns
   */
  std::vector<std::vector<AbstractExpressionRef>> point_lookup_keys_;

  // Add anything you want here for index lookup
  std::vector<IndexCondition> range_lookup_conds_;
  std::vector<AbstractExpressionRef> remaining_conds_;

 protected:
  auto PlanNodeToString() const -> std::string override {
    if (filter_predicate_) {
      std::string ss;
      ss += fmt::format("point_lookup_keys: [");
      for (const auto &key : point_lookup_keys_) {
        ss += "(";
        for (const auto &expr : key) {
          ss += expr->ToString() + "";
        }
        ss += ") ";
      }
      ss += "]\n";

      ss += "range_lookup_conds: [";
      for (const auto &cond : range_lookup_conds_) {
        ss += fmt::format("({} {} {})", cond.column_->ToString(), cond.type_, cond.constant_value_->ToString()) + " ";
      }
      ss += "]\n";

      ss += "remaining_conds: [";
      for (const auto &cond : remaining_conds_) {
        ss += cond->ToString() + " ";
      }
      ss += "]\n";

      return fmt::format("IndexScan {{ index_oid={}, filter={}, others={{{}}} }}", index_oid_, filter_predicate_, ss);
    }
    return fmt::format("IndexScan {{ index_oid={} }}", index_oid_);
  }
};

}  // namespace bustub
