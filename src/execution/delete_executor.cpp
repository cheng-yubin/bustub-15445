//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/delete_executor.h"
#include <memory>
#include "common/logger.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_{std::move(child_executor)} {}

void DeleteExecutor::Init() { 
  child_executor_->Init(); 
  
  // DeleteExecutor table XL : lock XL, unlock when submit/abort
  Transaction* txn = exec_ctx_->GetTransaction();
  TransactionManager* txn_mgr = exec_ctx_->GetTransactionManager();
  LockManager* lock_mgr = exec_ctx_->GetLockManager();

  try {
    bool locked = lock_mgr->LockTable(txn, LockManager::LockMode::EXCLUSIVE, plan_->TableOid());
    if (!locked) {
      std::string msg = "DeleteExecutor Lock exclusive fail.";
      throw ExecutionException(msg);
    }
  } catch (TransactionAbortException& exception) {
    LOG_DEBUG("%s", exception.GetInfo().c_str());
    txn_mgr->Abort(txn);

    std::string msg = "DeleteExecutor Lock exclusive aborted.";
    throw ExecutionException(msg);
  }
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  num_deleted_ = 0;

  TableInfo *tableinfo = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  auto indice_info = exec_ctx_->GetCatalog()->GetTableIndexes(tableinfo->name_);

  while (true) {
    const bool status = child_executor_->Next(tuple, rid);
    if (!status) {
      break;
    }

    num_deleted_++;
    bool status_delete = tableinfo->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
    if (!status_delete) {
      LOG_DEBUG("tuple delete fail");
    }

    for (auto index_info : indice_info) {
      auto key = tuple->KeyFromTuple(tableinfo->schema_, index_info->key_schema_,
                                     index_info->index_->GetMetadata()->GetKeyAttrs());
      index_info->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
    }
  }

  std::vector<Value> values{Value(GetOutputSchema().GetColumn(0).GetType(), num_deleted_)};
  *tuple = Tuple{values, &GetOutputSchema()};

  if (!output_) {
    output_ = true;
    return true;
  }

  return num_deleted_ != 0;
}

}  // namespace bustub
