//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/insert_executor.h"
#include <memory>
#include "common/logger.h"

namespace bustub {
void InsertExecutor::TryLock(LockManager::ResourceType type, LockManager::LockMode lock_mode, RID *rid) {
  Transaction *txn = exec_ctx_->GetTransaction();
  TransactionManager *txn_mgr = exec_ctx_->GetTransactionManager();
  LockManager *lock_mgr = exec_ctx_->GetLockManager();

  if (type == LockManager::ResourceType::TBALE) {
    try {
      bool locked = lock_mgr->LockTable(txn, lock_mode, plan_->TableOid());
      if (!locked) {
        txn_mgr->Abort(txn);
        std::string msg = "InsertExecutor Table IXL fail.";
        throw ExecutionException(msg);
      }
    } catch (TransactionAbortException &exception) {
      LOG_DEBUG("%s", exception.GetInfo().c_str());
      txn_mgr->Abort(txn);
      std::string msg = "InsertExecutor Table IXL aborted.";
      throw ExecutionException(msg);
    }
  } else {
    try {
      bool locked = lock_mgr->LockRow(txn, lock_mode, plan_->TableOid(), *rid);
      if (!locked) {
        txn_mgr->Abort(txn);
        std::string msg = "InsertExecutor Row SL fail.";
        throw ExecutionException(msg);
      }
    } catch (TransactionAbortException &exception) {
      LOG_DEBUG("%s", exception.GetInfo().c_str());
      txn_mgr->Abort(txn);
      std::string msg = "InsertExecutor Row SL aborted.";
      throw ExecutionException(msg);
    }
  }
}

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();

  // InsertExecutor table IXL: unlock when submit/abort
  TryLock(LockManager::ResourceType::TBALE, LockManager::LockMode::INTENTION_EXCLUSIVE);
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  num_inserted_ = 0;
  TableInfo *tableinfo = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  auto indice_info = exec_ctx_->GetCatalog()->GetTableIndexes(tableinfo->name_);

  while (true) {
    const auto status = child_executor_->Next(tuple, rid);
    if (!status) {
      break;
    }
    num_inserted_++;

    // insert the child tuple into table
    bool status_insert = tableinfo->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
    TryLock(LockManager::ResourceType::ROW, LockManager::LockMode::EXCLUSIVE, rid);

    if (!status_insert) {
      LOG_DEBUG(" tuple insert fail");
      return false;
    }

    // update the table index
    for (auto index_info : indice_info) {
      auto key = tuple->KeyFromTuple(tableinfo->schema_, index_info->key_schema_,
                                     index_info->index_->GetMetadata()->GetKeyAttrs());
      index_info->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
    }
  }

  // return the number of inserted tuples
  std::vector<Value> values{Value(GetOutputSchema().GetColumn(0).GetType(), num_inserted_)};
  *tuple = Tuple{values, &GetOutputSchema()};

  if (!output_) {
    output_ = true;
    return true;
  }

  return num_inserted_ != 0;
}

}  // namespace bustub
