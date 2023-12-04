//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"
namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  plan_ = plan;
  left_child_ = std::move(left_child);
  right_child_ = std::move(right_child);
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  ht_.clear();

  Tuple tuple;
  RID rid;
  while (right_child_->Next(&tuple, &rid)) {
    JoinHashKey joinhashkey;
    auto expressions = plan_->RightJoinKeyExpressions();
    for (auto const &express : expressions) {
      joinhashkey.joinkeys_.push_back(express->Evaluate(&tuple, right_child_->GetOutputSchema()));
    }
    ht_[joinhashkey].match_tuples_.push_back(tuple);
  }
}

void HashJoinExecutor::GetOutputTuple(Tuple *tuple, bool is_match) {
  Tuple right_tuple;
  if (is_match) {
    right_tuple = match_right_tuples_.back();
    match_right_tuples_.pop_back();
  }

  std::vector<Value> values;

  auto left_col_cnts = plan_->GetLeftPlan()->OutputSchema().GetColumnCount();
  auto right_col_cnts = plan_->GetRightPlan()->OutputSchema().GetColumnCount();

  for (uint32_t i = 0; i < left_col_cnts; ++i) {
    values.push_back(left_tuple_.GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
  }

  if (is_match) {
    for (uint32_t i = 0; i < right_col_cnts; ++i) {
      values.push_back(right_tuple.GetValue(&plan_->GetRightPlan()->OutputSchema(), i));
    }
  } else {
    for (uint32_t i = 0; i < right_col_cnts; ++i) {
      values.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
    }
  }
  *tuple = Tuple(values, &GetOutputSchema());
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (!match_right_tuples_.empty()) {
      GetOutputTuple(tuple, true);
      return true;
    }
    if (!left_child_->Next(&left_tuple_, rid)) {
      return false;
    }
    JoinHashKey joinhashkey;
    auto expressions = plan_->LeftJoinKeyExpressions();
    for (auto const &express : expressions) {
      joinhashkey.joinkeys_.push_back(express->Evaluate(&left_tuple_, left_child_->GetOutputSchema()));
    }
    if (ht_.count(joinhashkey) == 0 && plan_->GetJoinType() == JoinType::LEFT) {
      GetOutputTuple(tuple, false);
      return true;
    }
    if (ht_.count(joinhashkey) != 0) {
      match_right_tuples_ = ht_[joinhashkey].match_tuples_;
    }
  }
  return false;
}

}  // namespace bustub
