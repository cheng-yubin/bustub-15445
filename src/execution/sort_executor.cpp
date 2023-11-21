#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() { 
    child_executor_->Init();
    sort_table_.clear();

    Tuple tuple;
    RID rid;
    while (true) {
        const bool status = child_executor_->Next(&tuple, &rid);
        if (!status) {
            break;
        }
        sort_table_.push_back(std::pair<RID, Tuple>(rid, tuple));
    }
    
    cmp.set(plan_, &(child_executor_->GetOutputSchema()));
    std::sort(sort_table_.begin(), sort_table_.end(), cmp);
    output_count_ = 0;
}


auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (output_count_ >= sort_table_.size()) {
        return false;
    }

    *tuple = sort_table_[output_count_].second;
    *rid = sort_table_[output_count_].first;
    output_count_++;
    return true;
}

// static varibale should be init outside the class
SortExecutorComparator SortExecutor::cmp;

auto SortExecutorComparator::operator()(const std::pair<RID, Tuple>& entity1, const std::pair<RID, Tuple>& entity2) -> bool {
    auto order_by = plan_->GetOrderBy();
    for (const auto& p : order_by) {
        auto order_by_type = p.first;
        auto order_by_expr = p.second;
        auto val1 = order_by_expr->Evaluate(&(entity1.second), *scheme_);
        auto val2 = order_by_expr->Evaluate(&(entity2.second), *scheme_);
        if (val1.CompareNotEquals(val2) == CmpBool::CmpTrue) {
            return order_by_type==OrderByType::DESC ? val1.CompareGreaterThanEquals(val2)==CmpBool::CmpTrue :
                                                      val1.CompareLessThanEquals(val2)==CmpBool::CmpTrue;
        }
    }
    return true;
}


}  // namespace bustub
