//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>
#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */

class IndexScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new index scan executor.
   * @param exec_ctx the executor context
   * @param plan the index scan plan to be executed
   */
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

  auto TryLockTable(const bustub::LockManager::LockMode &lock_mode, const table_oid_t &oid) {
    std::string type;
    switch (lock_mode) {
      case bustub::LockManager::LockMode::EXCLUSIVE:
        type = "X";
        break;
      case bustub::LockManager::LockMode::INTENTION_EXCLUSIVE:
        type = "IX";
        break;
      case bustub::LockManager::LockMode::INTENTION_SHARED:
        type = "IS";
        break;
      case bustub::LockManager::LockMode::SHARED:
        type = "S";
        break;
      case bustub::LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
        type = "SIX";
        break;
      default:
        break;
    }
    try {
      bool success = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), lock_mode, oid);
      if (!success) {
        const std::string info = "indexscan table " + type + " lock fail";
        throw ExecutionException(info);
      }
    } catch (TransactionAbortException &e) {
      const std::string info = "indexscan table " + type + " lock fail";
      throw ExecutionException(info);
    }
  }

  auto TryUnLockTable(const table_oid_t &oid) {
    try {
      bool success = exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), oid);
      if (!success) {
        const std::string info = "indexscan table unlock fail";
        throw ExecutionException(info);
      }
    } catch (TransactionAbortException &e) {
      const std::string info = "indexscan table unlock fail";
      throw ExecutionException(info);
    }
  }

  auto TryLockRow(const bustub::LockManager::LockMode &lock_mode, const table_oid_t &oid, const RID &rid) {
    std::string type;
    switch (lock_mode) {
      case bustub::LockManager::LockMode::EXCLUSIVE:
        type = "X";
        break;
      case bustub::LockManager::LockMode::SHARED:
        type = "S";
        break;
      default:
        break;
    }
    try {
      auto success = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), lock_mode, oid, rid);
      if (!success) {
        const std::string info = "indexscan row " + type + " lock fail";
        throw ExecutionException(info);
      }
    } catch (TransactionAbortException &e) {
      const std::string info = "indexscan row " + type + " lock fail";
      throw ExecutionException(info);
    }
  }

  auto TryUnLockRow(const table_oid_t &oid, const RID &rid, bool force = false) {
    try {
      bool success = exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), oid, rid, force);
      if (!success) {
        const std::string info = "indexscan row unlock fail";
        throw ExecutionException(info);
      }
    } catch (TransactionAbortException &e) {
      const std::string info = "indexscan row unlock fail";
      throw ExecutionException(info);
    }
  }

 private:
  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;

  BPlusTreeIndexIteratorForTwoIntegerColumn iter_{nullptr, -1, -1};
  BPlusTreeIndexIteratorForTwoIntegerColumn end_{nullptr, -1, -1};
  TableInfo *table_info_;
  bool has_out_{false};
};
}  // namespace bustub
