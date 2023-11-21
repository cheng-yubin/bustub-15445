//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)),
    aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
    aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
    // build the hashtable first
    child_->Init();
    
    // no group, means there should be an output even no input
    if (plan_->GetGroupBys().empty()) {
        output = false;
    } else {
        output = true;
    }

    Tuple tuple;
    RID rid;
    while (true) {
        const bool status = child_->Next(&tuple, &rid);
        if (!status) {
            break;
        }

        auto key = MakeAggregateKey(&tuple);
        auto value = MakeAggregateValue(&tuple);
        aht_.InsertCombine(key, value);
    }
    aht_iterator_ = aht_.Begin();

    output = false;
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool { 

    // output tuple by tuple
    if (aht_iterator_ != aht_.End()) {
        // LOG_DEBUG("output schema: %s", GetOutputSchema().ToString().c_str());
        
        std::vector<Value> vec{aht_iterator_.Key().group_bys_};
        vec.insert(vec.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());
        *tuple = Tuple{vec, &GetOutputSchema()};
        ++aht_iterator_;
        output = true;
        return true;
    }
    
    // special case of no group and no imput
    if (!output) {
        auto init_value = aht_.GenerateInitialAggregateValue();
        *tuple = Tuple{init_value.aggregates_, &GetOutputSchema()};
        output = true;
        return true;
    }

    return false;
 }

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
