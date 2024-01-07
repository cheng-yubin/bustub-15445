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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iter_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->Begin(exec_ctx_->GetTransaction())),
      end_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->End()) {}

void SeqScanExecutor::Init() {
  // SeqScanExecutor table SL
  // REPEATABLE_READ: lock SL, unlock when submit/abort
  // READ_COMMITTED: lock SL, unlock when finishing scan
  // READ_UNCOMMITTED: never lock SL
  
  Transaction* txn = exec_ctx_->GetTransaction();
  TransactionManager* txn_mgr = exec_ctx_->GetTransactionManager();
  LockManager* lock_mgr = exec_ctx_->GetLockManager();

  // REPEATABLE_READ || READ_COMMITTED: lock SL
  if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      bool locked = lock_mgr->LockTable(txn, LockManager::LockMode::SHARED, plan_->GetTableOid());
      if (!locked) {
        std::string msg = "SeqScanExecutor Lock shared fail.";
        throw ExecutionException(msg);
      }
    } catch (TransactionAbortException& exception) {
      LOG_DEBUG("%s", exception.GetInfo().c_str());
      txn_mgr->Abort(txn);

      std::string msg = "SeqScanExecutor Lock shared aborted.";
      throw ExecutionException(msg);
    }
  }

  iter_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ != end_) {
    *tuple = *iter_;
    *rid = iter_->GetRid();
    ++iter_;
    return true;
  }

  Transaction* txn = exec_ctx_->GetTransaction();
  LockManager* lock_mgr = exec_ctx_->GetLockManager();
  TransactionManager* txn_mgr = exec_ctx_->GetTransactionManager();

  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    try {
      LOG_DEBUG("seq_scan finished, unlock table");
      lock_mgr->UnlockTable(txn, plan_->GetTableOid());
    } catch (TransactionAbortException& exception) {
      LOG_DEBUG("%s", exception.GetInfo().c_str());
      txn_mgr->Abort(txn);
    }
  }
  return false;
}

}  // namespace bustub
