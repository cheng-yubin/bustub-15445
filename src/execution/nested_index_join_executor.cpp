//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() { 
  child_executor_->Init();
  
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
  
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  Tuple left_tuple;
  Tuple right_tuple;

  while (true) {
    const bool status = child_executor_->Next(&left_tuple, rid);
    if (!status) {
      return false;
    }

    auto key_expr = plan_->KeyPredicate();
    auto key = key_expr->Evaluate(&left_tuple, child_executor_->GetOutputSchema());
    
    std::vector<RID> res_rid;
    index_info_->index_->ScanKey(Tuple(std::vector<Value>{key}, index_info_->index_->GetKeySchema()), &res_rid, exec_ctx_->GetTransaction());

    if (!res_rid.empty()) {
      table_info_->table_->GetTuple(res_rid[0], &right_tuple, exec_ctx_->GetTransaction());
      
      std::vector<Value> vec;
      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        vec.push_back(left_tuple.GetValue(&(child_executor_->GetOutputSchema()), i));
      }
      for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); ++i) {
        vec.push_back(right_tuple.GetValue(&(table_info_->schema_), i));
      }
      *tuple = Tuple(vec, &GetOutputSchema());

      return true;
    }

    else {
      if (plan_->GetJoinType() == JoinType::INNER) {
        continue;
      }
      else if (plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> vec;
        for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          vec.push_back(left_tuple.GetValue(&(child_executor_->GetOutputSchema()), i));
        }
        for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); ++i) {
          vec.push_back(ValueFactory::GetNullValueByType(table_info_->schema_.GetColumn(i).GetType()));
        }
        *tuple = Tuple(vec, &GetOutputSchema());

        return true;
      }
    }

  }

  return false;
}

}  // namespace bustub
