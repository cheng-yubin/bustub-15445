//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <queue>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
class TopNExecutorComparator {
 public:
  TopNExecutorComparator(const TopNPlanNode *plan, const Schema *scheme) : plan_(plan), scheme_(scheme) {}

  // true when
  auto operator()(const std::pair<RID, Tuple> &entity1, const std::pair<RID, Tuple> &entity2) -> bool;

 private:
  const TopNPlanNode *plan_;
  const Schema *scheme_;
};

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The topn plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the topn */
  void Init() override;

  /**
   * Yield the next tuple from the topn.
   * @param[out] tuple The next tuple produced by the topn
   * @param[out] rid The next tuple RID produced by the topn
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the topn */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The topn plan node to be executed */
  const TopNPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;

  size_t heap_size_{0};

  std::priority_queue<std::pair<RID, Tuple>, std::vector<std::pair<RID, Tuple>>, TopNExecutorComparator> heap_;

  int output_count_{0};
  std::vector<std::pair<RID, Tuple>> res_;
};

}  // namespace bustub
