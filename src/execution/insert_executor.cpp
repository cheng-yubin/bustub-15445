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

#include <memory>
#include "common/logger.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { 
    child_executor_->Init();
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    num_inserted = 0;
    TableInfo* tableinfo = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
    auto indice_info = exec_ctx_->GetCatalog()->GetTableIndexes(tableinfo->name_);

    while (true) {
        const auto status = child_executor_->Next(tuple, rid);
        if (!status) {
           break;
        }

        num_inserted++;

        // insert the child tuple into table
        bool status_insert = tableinfo->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());

        if (!status_insert) {
            LOG_DEBUG(" tuple insert fail");
            return false;
        }

        // update the table index
        for(auto index_info : indice_info) {
            auto key = tuple->KeyFromTuple(tableinfo->schema_, index_info->key_schema_, 
                                            index_info->index_->GetMetadata()->GetKeyAttrs());
            index_info->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
        }
    }

    // return the number of inserted tuples
    std::vector<Value> values{Value(GetOutputSchema().GetColumn(0).GetType(), num_inserted)};
    *tuple = Tuple{values, &GetOutputSchema()};
 
    if (!output) {
        output = true;
        return true;
    }

    return num_inserted != 0;
}

}  // namespace bustub
