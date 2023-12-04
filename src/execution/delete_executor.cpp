//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());
  index_infos_ = catalog->GetTableIndexes(table_info_->name_);
  has_out_ = false;
  child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (has_out_) {
    return false;
  }
  int nums = 0;
  while (child_executor_->Next(tuple, rid)) {
    auto tuplemeta = table_info_->table_->GetTupleMeta(*rid);
    tuplemeta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(tuplemeta, *rid);

    auto twr = TableWriteRecord{table_info_->oid_, *rid, table_info_->table_.get()};
    twr.wtype_ = WType::DELETE;
    exec_ctx_->GetTransaction()->GetWriteSet()->push_back(twr);

    nums++;

    for (auto &info : index_infos_) {
      Tuple partial_tuple =
          tuple->KeyFromTuple(table_info_->schema_, *(info->index_->GetKeySchema()), info->index_->GetKeyAttrs());
      info->index_->DeleteEntry(partial_tuple, *rid, exec_ctx_->GetTransaction());
      auto iwr = IndexWriteRecord{*rid,          table_info_->oid_, WType::DELETE,
                                  partial_tuple, info->index_oid_,  exec_ctx_->GetCatalog()};
      exec_ctx_->GetTransaction()->GetIndexWriteSet()->push_back(iwr);
    }
  }
  std::vector<Value> values{};
  values.emplace_back(Value(INTEGER, nums));
  *tuple = Tuple(values, &GetOutputSchema());
  has_out_ = true;
  return true;
}

}  // namespace bustub
