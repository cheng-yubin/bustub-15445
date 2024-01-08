#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      heap_(TopNExecutorComparator(plan_, &(child_executor_->GetOutputSchema()))) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  heap_ = std::priority_queue<std::pair<RID, Tuple>, std::vector<std::pair<RID, Tuple>>, TopNExecutorComparator>(
      TopNExecutorComparator(plan_, &(child_executor_->GetOutputSchema())));
  heap_size_ = 0;
  res_.clear();

  Tuple tuple;
  RID rid;
  while (true) {
    const bool status = child_executor_->Next(&tuple, &rid);
    if (!status) {
      break;
    }

    heap_.push(std::pair<RID, Tuple>(rid, tuple));
    if (heap_size_ == plan_->GetN()) {
      heap_.pop();
    } else {
      heap_size_++;
    }
  }

  while (!heap_.empty()) {
    res_.push_back(heap_.top());
    heap_.pop();
  }
  output_count_ = res_.size() - 1;
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (output_count_ < 0) {
    return false;
  }
  *tuple = res_[output_count_].second;
  *rid = res_[output_count_].first;
  --output_count_;

  return true;
}

auto TopNExecutorComparator::operator()(const std::pair<RID, Tuple> &entity1, const std::pair<RID, Tuple> &entity2)
    -> bool {
  auto order_by = plan_->GetOrderBy();
  for (const auto &p : order_by) {
    auto order_by_type = p.first;
    auto order_by_expr = p.second;
    auto val1 = order_by_expr->Evaluate(&(entity1.second), *scheme_);
    auto val2 = order_by_expr->Evaluate(&(entity2.second), *scheme_);
    if (val1.CompareNotEquals(val2) == CmpBool::CmpTrue) {
      return order_by_type == OrderByType::DESC ? val1.CompareGreaterThanEquals(val2) == CmpBool::CmpTrue
                                                : val1.CompareLessThanEquals(val2) == CmpBool::CmpTrue;
    }
  }
  return true;
}

}  // namespace bustub
