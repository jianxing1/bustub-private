//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  // 上锁？
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());
  index_infos_ = catalog->GetTableIndexes(table_info_->name_);
  has_out_ = false;
  child_executor_->Init();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (has_out_) {
    return false;
  }
  int nums = 0;
  while (child_executor_->Next(tuple, rid)) {
    std::vector<Value> values{};
    for (auto &express : plan_->target_expressions_) {
      values.push_back(express->Evaluate(tuple, table_info_->schema_));
    }
    Tuple new_tuple = Tuple(values, &table_info_->schema_);

    // P4
    auto new_tuplemeta = TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false};
    table_info_->table_->UpdateTupleInPlaceUnsafe(new_tuplemeta, new_tuple, *rid);
    nums++;

    auto twr = TableWriteRecord{table_info_->oid_, *rid, table_info_->table_.get()};
    twr.wtype_ = WType::UPDATE;

    exec_ctx_->GetTransaction()->GetWriteSet()->push_back(twr);

    for (auto &info : index_infos_) {
      Tuple partial_tuple =
          tuple->KeyFromTuple(table_info_->schema_, *(info->index_->GetKeySchema()), info->index_->GetKeyAttrs());
      info->index_->DeleteEntry(partial_tuple, *rid, exec_ctx_->GetTransaction());

      auto iwr1 = IndexWriteRecord{*rid,          table_info_->oid_, WType::DELETE,
                                   partial_tuple, info->index_oid_,  exec_ctx_->GetCatalog()};
      exec_ctx_->GetTransaction()->GetIndexWriteSet()->push_back(iwr1);

      Tuple partial_new_tuple =
          new_tuple.KeyFromTuple(table_info_->schema_, *(info->index_->GetKeySchema()), info->index_->GetKeyAttrs());
      info->index_->InsertEntry(partial_new_tuple, *rid, exec_ctx_->GetTransaction());

      auto iwr2 = IndexWriteRecord{
          *rid, table_info_->oid_, WType::INSERT, partial_new_tuple, info->index_oid_, exec_ctx_->GetCatalog()};
      exec_ctx_->GetTransaction()->GetIndexWriteSet()->push_back(iwr2);
    }
  }

  std::vector<Value> values{};
  values.emplace_back(Value(INTEGER, nums));
  *tuple = Tuple(values, &GetOutputSchema());

  has_out_ = true;

  return true;
}

}  // namespace bustub
