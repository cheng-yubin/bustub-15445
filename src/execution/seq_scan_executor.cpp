//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

void SeqScanExecutor::TryLock(LockManager::ResourceType type, LockManager::LockMode lock_mode, RID *rid) {
  Transaction *txn = exec_ctx_->GetTransaction();
  TransactionManager *txn_mgr = exec_ctx_->GetTransactionManager();
  LockManager *lock_mgr = exec_ctx_->GetLockManager();

  if (type == LockManager::ResourceType::TBALE) {
    try {
      bool locked = lock_mgr->LockTable(txn, lock_mode, plan_->GetTableOid());
      if (!locked) {
        txn_mgr->Abort(txn);
        std::string msg = "SeqScanExecutor Table ISL fail.";
        throw ExecutionException(msg);
      }
    } catch (TransactionAbortException &exception) {
      LOG_DEBUG("%s", exception.GetInfo().c_str());
      txn_mgr->Abort(txn);
      std::string msg = "SeqScanExecutor Table ISL aborted.";
      throw ExecutionException(msg);
    }
  } else {
    try {
      bool locked = lock_mgr->LockRow(txn, lock_mode, plan_->GetTableOid(), *rid);
      if (!locked) {
        txn_mgr->Abort(txn);
        std::string msg = "SeqScanExecutor Row SL fail.";
        throw ExecutionException(msg);
      }
    } catch (TransactionAbortException &exception) {
      LOG_DEBUG("%s", exception.GetInfo().c_str());
      txn_mgr->Abort(txn);
      std::string msg = "SeqScanExecutor Row SL aborted.";
      throw ExecutionException(msg);
    }
  }
}

void SeqScanExecutor::TryUnlock(LockManager::ResourceType type, RID *rid) {
  Transaction *txn = exec_ctx_->GetTransaction();
  TransactionManager *txn_mgr = exec_ctx_->GetTransactionManager();
  LockManager *lock_mgr = exec_ctx_->GetLockManager();

  if (type == LockManager::ResourceType::TBALE) {
    try {
      LOG_DEBUG("seq_scan try to unlock table");
      lock_mgr->UnlockTable(txn, plan_->GetTableOid());
    } catch (TransactionAbortException &exception) {
      LOG_DEBUG("%s", exception.GetInfo().c_str());
      txn_mgr->Abort(txn);
    }
  } else {
    try {
      LOG_DEBUG("seq_scan try to unlock row");
      lock_mgr->UnlockRow(txn, plan_->GetTableOid(), *rid);
    } catch (TransactionAbortException &exception) {
      LOG_DEBUG("%s", exception.GetInfo().c_str());
      txn_mgr->Abort(txn);
    }
  }
}

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iter_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->Begin(exec_ctx_->GetTransaction())),
      end_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->End()) {}

void SeqScanExecutor::Init() {
  // SeqScanExecutor table IS, row S
  // REPEATABLE_READ: table IS, row S, unlock when submit/abort
  // READ_COMMITTED: table IS, row S, unlock when finishing scan
  // READ_UNCOMMITTED: never lock SL

  // REPEATABLE_READ || READ_COMMITTED: lock IS
  auto txn = exec_ctx_->GetTransaction();
  if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ ||
       txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) &&
      !txn->IsTableIntentionExclusiveLocked(plan_->GetTableOid()) &&
      !txn->IsTableIntentionSharedLocked(plan_->GetTableOid())) {
    TryLock(LockManager::ResourceType::TBALE, LockManager::LockMode::INTENTION_SHARED);
  }

  iter_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->Begin(txn);
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto txn = exec_ctx_->GetTransaction();

  if (iter_ != end_) {
    *rid = iter_->GetRid();
    bool obtain_lock = false;
    // row SL
    if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ ||
         txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) &&
        !txn->IsRowSharedLocked(plan_->GetTableOid(), *rid) && !txn->IsRowExclusiveLocked(plan_->GetTableOid(), *rid)) {
      obtain_lock = true;
      TryLock(LockManager::ResourceType::ROW, LockManager::LockMode::SHARED, rid);
    }

    // Read tuple
    *tuple = *iter_;
    ++iter_;

    if (obtain_lock && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      // try unlock row
      TryUnlock(LockManager::ResourceType::ROW, rid);
    }

    return true;
  }

  return false;
}

}  // namespace bustub
