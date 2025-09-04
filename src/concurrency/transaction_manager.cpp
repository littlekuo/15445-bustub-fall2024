//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Begins a new transaction.
 * @param isolation_level an optional isolation level of the transaction.
 * @return an initialized transaction
 */
auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_ = last_commit_ts_.load(std::memory_order_acquire);

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

/** @brief Verify if a txn satisfies serializability. We will not test this function and you can change / remove it as
 * you want. */
auto TransactionManager::VerifyTxn(Transaction *txn) -> bool {
  auto matches_predicate = [](const Tuple &tuple, const Schema &schema,
                              std::vector<AbstractExpressionRef> &scan_predicates) -> bool {
    for (const auto &pred : scan_predicates) {
      auto res = pred->Evaluate(&tuple, schema);
      if (!res.IsNull() && res.GetAs<bool>()) {
        return true;
      }
    }
    return false;
  };

  timestamp_t read_ts;
  {
    std::unique_lock<std::mutex> lck(txn->latch_);
    read_ts = txn->read_ts_.load();
    if (txn->GetWriteSets().empty()) {
      // read-only txn
      return true;
    }
  }

  std::vector<std::pair<table_oid_t, std::unordered_set<RID>>> all_writes;
  std::vector<std::pair<table_oid_t, std::unordered_set<RID>>> related_writes;
  {
    // collect all writes
    std::shared_lock<std::shared_mutex> lck(txn_map_mutex_);
    for (auto &it : txn_map_) {
      auto &other_txn = it.second;
      std::unique_lock<std::mutex> txn_lck(other_txn->latch_);
      auto state = other_txn->state_.load();
      if (state != TransactionState::COMMITTED || other_txn->commit_ts_ <= read_ts) {
        continue;
      }
      for (auto &w : other_txn->write_set_) {
        all_writes.emplace_back(w.first, w.second);
      }
    }
  }
  {
    // collect related writes
    std::unique_lock<std::mutex> txn_lck(txn->latch_);
    for (auto &w : all_writes) {
      if (txn->scan_predicates_.find(w.first) == txn->scan_predicates_.end()) {
        // not related table, skip
        continue;
      }
      related_writes.emplace_back(std::move(w));
    }
  }

  {
    for (auto &w : related_writes) {
      auto table = catalog_->GetTable(w.first);
      for (auto &rid : w.second) {
        auto [meta, tuple, undo_link] = GetTupleAndUndoLink(this, table->table_.get(), rid);
        BUSTUB_ASSERT(meta.ts_ > read_ts, "should not happen");
        bool is_deleted = meta.is_deleted_;
        auto ts = meta.ts_;
        auto cur_tuple = tuple;
        bool conflict = false;
        while (ts > read_ts) {
          // no undo log (if ever exist, the undo log must not garbage collected)
          if (!undo_link.has_value() || !undo_link->IsValid()) {
            // inserted
            if (!is_deleted && matches_predicate(cur_tuple, table->schema_, txn->scan_predicates_[w.first])) {
              conflict = true;
            }
            break;
          }
          auto undo_log = GetUndoLog(undo_link.value());
          undo_link = undo_log.prev_version_;
          auto pre_tuple = ReconstructTuple(&table->schema_, cur_tuple, meta, {undo_log});
          if (is_deleted) {
            // deleted
            if (pre_tuple.has_value() &&
                (matches_predicate(pre_tuple.value(), table->schema_, txn->scan_predicates_[w.first]))) {
              conflict = true;
              break;
            }
          } else if (matches_predicate(cur_tuple, table->schema_, txn->scan_predicates_[w.first])) {
            conflict = true;
            break;
          }

          if (pre_tuple.has_value() &&
              matches_predicate(pre_tuple.value(), table->schema_, txn->scan_predicates_[w.first])) {
            conflict = true;
            break;
          }
          is_deleted = !pre_tuple.has_value();
          ts = undo_log.ts_;
          cur_tuple = pre_tuple.value_or(Tuple{});
        }
        if (conflict) {
          return false;
        }
      }
    }
  }
  return true;
}

/**
 * Commits a transaction.
 * @param txn the transaction to commit, the txn will be managed by the txn manager so no need to delete it by
 * yourself
 */
auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  auto commit_ts = last_commit_ts_.load(std::memory_order_acquire) + 1;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!
  txn->latch_.lock();
  for (auto &w : txn->write_set_) {
    auto table = catalog_->GetTable(w.first);
    for (auto &rid : w.second) {
      auto page_write_guard = table->table_->AcquireTablePageWriteLock(rid);
      auto page = page_write_guard.AsMut<TablePage>();
      auto tuple_meta = table->table_->GetTupleMetaWithLockAcquired(rid, page);
      page->UpdateTupleMeta(TupleMeta{commit_ts, tuple_meta.is_deleted_}, rid);
    }
  }
  txn->latch_.unlock();

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  txn->commit_ts_ = commit_ts;
  last_commit_ts_.fetch_add(1, std::memory_order_release);
  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

/**
 * Aborts a transaction
 * @param txn the transaction to abort, the txn will be managed by the txn manager so no need to delete it by yourself
 */
void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!
  for (auto &w : txn->write_set_) {
    auto table = catalog_->GetTable(w.first);
    for (auto &rid : w.second) {
      auto [meta, tuple, undo_link] = GetTupleAndUndoLink(this, table->table_.get(), rid);
      if (undo_link.has_value() && undo_link->IsValid()) {
        auto log = GetUndoLog(undo_link.value());
        auto rebuild_tuple = ReconstructTuple(&table->schema_, tuple, meta, {log});
        if (!rebuild_tuple.has_value()) {
          auto new_meta = TupleMeta{log.ts_, true};
          UpdateTupleAndUndoLink(this, rid, log.prev_version_, table->table_.get(), txn, new_meta, tuple);
          continue;
        }
        auto new_meta = TupleMeta{log.ts_, false};
        UpdateTupleAndUndoLink(this, rid, log.prev_version_, table->table_.get(), txn, new_meta, rebuild_tuple.value());
      } else {
        auto new_meta = TupleMeta{0, true};
        UpdateTupleAndUndoLink(this, rid, std::nullopt, table->table_.get(), txn, new_meta, tuple);
      }
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

/** @brief Stop-the-world garbage collection. Will be called only when all transactions are not accessing the table
 * heap. */
void TransactionManager::GarbageCollection() {
  auto water_mark = running_txns_.GetWatermark();
  std::unordered_map<txn_id_t, size_t> invisible_undo_logs_cnt;
  std::vector<std::pair<table_oid_t, RID>> tuple_infos;
  std::unordered_set<RID> invisible_page_versions;
  {
    // collect all rids
    std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
    for (auto &it : txn_map_) {
      auto &txn = it.second;
      std::unique_lock<std::mutex> txn_lck(txn->latch_);
      for (auto &w : txn->write_set_) {
        for (auto &rid : w.second) {
          tuple_infos.emplace_back(w.first, rid);
        }
      }
    }
  }
  {
    // collect all invisible page versions
    for (auto &info : tuple_infos) {
      auto table = catalog_->GetTable(info.first);
      auto page_read_guard = table->table_->AcquireTablePageReadLock(info.second);
      auto page = page_read_guard.As<TablePage>();
      auto [meta, tuple] = page->GetTuple(info.second);
      if (meta.ts_ <= water_mark) {
        invisible_page_versions.insert(info.second);
      }
    }
  }
  {
    // collect all invisible undo logs
    std::unique_lock<std::shared_mutex> lck(version_info_mutex_);
    for (auto &it : version_info_) {
      auto &page_version_info = it.second;
      std::unique_lock<std::shared_mutex> page_lck(page_version_info->mutex_);
      for (auto &vit : page_version_info->prev_link_) {
        RID rid{it.first, static_cast<uint32_t>(vit.first)};
        auto meet_first = invisible_page_versions.find(rid) != invisible_page_versions.end();
        auto link = vit.second;
        std::optional<UndoLog> log = std::nullopt;
        for (; link.IsValid(); link = log.value().prev_version_) {
          log = GetUndoLogOptional(link);
          if (!log.has_value()) {
            break;
          }
          if (meet_first) {
            invisible_undo_logs_cnt[link.prev_txn_]++;
            continue;
          }
          if (log.value().ts_ <= water_mark) {
            meet_first = true;
            continue;
          }
        }
      }
    }
  }
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  for (auto it = txn_map_.begin(); it != txn_map_.end();) {
    auto &txn = it->second;
    std::unique_lock<std::mutex> txn_lck(txn->latch_);
    auto state = txn->state_.load();
    auto is_complete = state == TransactionState::COMMITTED || state == TransactionState::ABORTED;
    if (!is_complete) {
      it++;
      continue;
    }
    if (txn->undo_logs_.empty() || invisible_undo_logs_cnt[txn->GetTransactionId()] == txn->undo_logs_.size()) {
      it = txn_map_.erase(it);
    } else {
      it++;
    }
  }
}

}  // namespace bustub
