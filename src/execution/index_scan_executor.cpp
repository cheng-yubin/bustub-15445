//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
    auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
    table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);
    auto tree = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());
    
    iter_ = tree->GetBeginIterator();
    end_ = tree->GetEndIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (iter_ == end_) {
        return false;
    }

    *rid = (*iter_).second;
    table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
    ++iter_;
    return true;
 }

}  // namespace bustub
