//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  RID rid;
  bool left_status = left_executor_->Next(&left_tuple_, &rid);
  end_ = !left_status;

  left_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (end_) {
    return false;
  }

  // Inner join
  if (plan_->GetJoinType() == JoinType::INNER) {
    auto &join_expr = plan_->Predicate();

    while (!end_) {
      const bool right_status = right_executor_->Next(&right_tuple_, rid);
      if (!right_status) {
        bool left_status = left_executor_->Next(&left_tuple_, rid);
        end_ = !left_status;

        right_executor_->Init();
        continue;
      }

      auto value = join_expr.EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple_,
                                          right_executor_->GetOutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        std::vector<Value> vec;
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          vec.push_back(left_tuple_.GetValue(&(left_executor_->GetOutputSchema()), i));
        }
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          vec.push_back(right_tuple_.GetValue(&(right_executor_->GetOutputSchema()), i));
        }
        *tuple = Tuple(vec, &GetOutputSchema());
        return true;
      }
    }

    return false;
  }

  if (plan_->GetJoinType() == JoinType::LEFT) {
    auto &join_expr = plan_->Predicate();

    while (!end_) {
      const bool right_status = right_executor_->Next(&right_tuple_, rid);
      if (!right_status) {
        if (!left_) {
          std::vector<Value> vec;
          for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
            vec.push_back(left_tuple_.GetValue(&(left_executor_->GetOutputSchema()), i));
          }
          for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
            vec.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
          }
          *tuple = Tuple(vec, &GetOutputSchema());

          bool left_status = left_executor_->Next(&left_tuple_, rid);
          end_ = !left_status;
          right_executor_->Init();

          return true;
        }

        bool left_status = left_executor_->Next(&left_tuple_, rid);
        end_ = !left_status;
        left_ = false;
        right_executor_->Init();
        continue;
      }

      auto value = join_expr.EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple_,
                                          right_executor_->GetOutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        std::vector<Value> vec;
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          vec.push_back(left_tuple_.GetValue(&(left_executor_->GetOutputSchema()), i));
        }
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          vec.push_back(right_tuple_.GetValue(&(right_executor_->GetOutputSchema()), i));
        }
        *tuple = Tuple(vec, &GetOutputSchema());
        left_ = true;
        return true;
      }
    }
    return false;
  }

  return false;
}

}  // namespace bustub
