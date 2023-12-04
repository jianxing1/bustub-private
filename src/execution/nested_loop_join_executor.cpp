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
    : AbstractExecutor(exec_ctx) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  plan_ = plan;
  left_executor_ = std::move(left_executor);
  right_executor_ = std::move(right_executor);
}

void NestedLoopJoinExecutor::Init() {
  Tuple tuple;
  RID rid;
  left_executor_->Init();
  right_executor_->Init();

  left_tuple_ = new Tuple();
  right_tuple_ = new Tuple();

  left_executor_->Next(left_tuple_, &rid);
  match_ = false;
}

void NestedLoopJoinExecutor::GetOutputTuple(Tuple *tuple, bool is_match) {
  std::vector<Value> values;

  auto left_col_cnts = plan_->GetLeftPlan()->OutputSchema().GetColumnCount();
  auto right_col_cnts = plan_->GetRightPlan()->OutputSchema().GetColumnCount();

  for (uint32_t i = 0; i < left_col_cnts; ++i) {
    values.push_back(left_tuple_->GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
  }

  if (is_match) {
    for (uint32_t i = 0; i < right_col_cnts; ++i) {
      values.push_back(right_tuple_->GetValue(&plan_->GetRightPlan()->OutputSchema(), i));
    }
  } else {
    for (uint32_t i = 0; i < right_col_cnts; ++i) {
      values.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
    }
  }
  *tuple = Tuple(values, &GetOutputSchema());
  match_ = true;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    while (right_executor_->Next(right_tuple_, rid)) {
      auto value = plan_->Predicate()->EvaluateJoin(left_tuple_, plan_->GetLeftPlan()->OutputSchema(), right_tuple_,
                                                    plan_->GetRightPlan()->OutputSchema());
      if (value.CompareEquals(ValueFactory::GetBooleanValue(true)) == CmpBool::CmpTrue) {
        GetOutputTuple(tuple, true);
        return true;
      }
    }
    if (!match_ && plan_->GetJoinType() == JoinType::LEFT) {
      GetOutputTuple(tuple, false);
      right_executor_->Init();
      return true;
    }
    if (!left_executor_->Next(left_tuple_, rid)) {
      delete left_tuple_;
      left_tuple_ = nullptr;

      delete right_tuple_;
      right_tuple_ = nullptr;
      return false;
    }
    right_executor_->Init();
    match_ = false;
  }
  return true;
}

}  // namespace bustub
