//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"
// #include "lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) { max_size_ = num_frames; }

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (Size() == 0) {
    return false;
  }
  for (auto it = new_frame_.rbegin(); it != new_frame_.rend(); ++it) {
    auto frame = *it;
    if (evictable_[frame]) {
      recorded_cnt_[frame] = 0;
      new_locate_.erase(frame);
      new_frame_.remove(frame);
      *frame_id = frame;
      curr_size_--;
      hist_[frame].clear();
      return true;
    }
  }
  for (auto it = cache_frame_.begin(); it != cache_frame_.end(); ++it) {
    auto frame = (*it).first;
    if (evictable_[frame]) {
      recorded_cnt_[frame] = 0;
      cache_frame_.erase(it);
      cache_locate_.erase(frame);
      *frame_id = frame;
      curr_size_--;
      hist_[frame].clear();
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw Exception("[RecordAccess] frame id is invalid");
  }
  current_timestamp_++;
  auto cnt = ++recorded_cnt_[frame_id];
  hist_[frame_id].push_back(current_timestamp_);
  if (cnt == 1) {
    if (curr_size_ == max_size_) {
      frame_id_t frame;
      Evict(&frame);
    }
    evictable_[frame_id] = true;
    curr_size_++;
    new_frame_.push_front(frame_id);
    new_locate_[frame_id] = new_frame_.begin();
  }
  if (cnt == k_) {
    new_frame_.erase(new_locate_[frame_id]);
    new_locate_.erase(frame_id);

    auto kth_time = hist_[frame_id].front();
    k_time new_cache(frame_id, kth_time);
    auto it = std::upper_bound(cache_frame_.begin(), cache_frame_.end(), new_cache, CmpTimestamp);
    it = cache_frame_.insert(it, new_cache);
    cache_locate_[frame_id] = it;
    return;
  }
  if (cnt > k_) {
    hist_[frame_id].erase(hist_[frame_id].begin());
    cache_frame_.erase(cache_locate_[frame_id]);
    auto kth_time = hist_[frame_id].front();
    k_time new_cache(frame_id, kth_time);
    auto it = std::upper_bound(cache_frame_.begin(), cache_frame_.end(), new_cache, CmpTimestamp);
    it = cache_frame_.insert(it, new_cache);
    cache_locate_[frame_id] = it;
    return;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);

  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw Exception("[SetEvictable] frame id is invalid");
  }
  if (recorded_cnt_[frame_id] == 0) {
    return;
  }
  auto status = evictable_[frame_id];
  evictable_[frame_id] = set_evictable;
  if (status && !set_evictable) {
    max_size_--;
    curr_size_--;
  }
  if (!status && set_evictable) {
    max_size_++;
    curr_size_++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw Exception("[Remove] frame id is invalid");
  }
  if (!evictable_[frame_id]) {
    throw Exception("[Remove] frame id is non-evictable");
  }
  auto cnt = recorded_cnt_[frame_id];
  if (cnt == 0) {
    return;
  }
  if (cnt < k_) {
    new_frame_.erase(new_locate_[frame_id]);
    new_locate_.erase(frame_id);
    recorded_cnt_[frame_id] = 0;
    hist_[frame_id].clear();
    curr_size_--;
  } else {
    cache_frame_.erase(cache_locate_[frame_id]);
    cache_locate_.erase(frame_id);
    recorded_cnt_[frame_id] = 0;
    hist_[frame_id].clear();
    curr_size_--;
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

auto LRUKReplacer::CmpTimestamp(const k_time &t1, const k_time &t2) -> bool { return t1.second < t2.second; }

}  // namespace bustub
