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

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  // LOG_DEBUG("num_frames: %lu, k = %lu", num_frames, k);
  for (frame_id_t i = 0; i < frame_id_t(num_frames); i++) {
    access_cnt_[i] = 0;
    access_hist_[i] = std::list<size_t>();
    evictable_[i] = false;
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  // LOG_DEBUG("evict");
  if (curr_size_ == 0) {
    return false;
  }

  if (!frames_new_.empty()) {
    *frame_id = frames_new_.front();
    frames_new_.pop_front();
    locale_new_.erase(*frame_id);

    access_cnt_[*frame_id] = 0;
    access_hist_[*frame_id].clear();

    curr_size_--;
    evictable_[*frame_id] = false;

    // LOG_DEBUG("evict from visit list, frame_id = %d", *frame_id);
    return true;
  }

  if (!frames_k_.empty()) {
    *frame_id = frames_k_.front().first;
    frames_k_.pop_front();
    locale_k_.erase(*frame_id);

    access_cnt_[*frame_id] = 0;
    access_hist_[*frame_id].clear();

    curr_size_--;
    evictable_[*frame_id] = false;

    // LOG_DEBUG("evict from cache list, frame_id = %d", *frame_id);
    return true;
  }

  return false;
}

auto LRUKReplacer::CmpTimeStamp(const LRUKReplacer::k_time &t1, const LRUKReplacer::k_time &t2) -> bool {
  return t1.second < t2.second;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // LOG_DEBUG("RecordAccess: %d", frame_id);
  BUSTUB_ASSERT(replacer_size_ - frame_id > 0, "required frame_id is larger than up bound");
  // LOG_DEBUG("record access, frame id = %d, evictable = %d", frame_id, evictable_[frame_id]);
  // the original count of access
  size_t cnt = access_cnt_[frame_id];

  // fresh the access record
  access_cnt_[frame_id] += 1;
  if (access_hist_[frame_id].size() >= k_) {
    access_hist_[frame_id].pop_front();
  }
  access_hist_[frame_id].push_back(curr_timestamp_++);

  // If NOT evictable, no need to change lists
  if (!evictable_[frame_id]) {
    return;
  }

  // visit list to visit list
  if (cnt < k_ - 1) {
    auto iter = locale_new_.find(frame_id);
    if (iter == locale_new_.end()) {
      LOG_DEBUG("page is not in the visit list");
    }
    frames_new_.erase(iter->second);
    frames_new_.push_back(frame_id);
    locale_new_[frame_id] = std::prev(frames_new_.end());
  }

  // vistt list to cache list
  if (cnt == k_ - 1) {
    auto iter = locale_new_.find(frame_id);
    if (iter == locale_new_.end()) {
      LOG_DEBUG("page is not in the visit list");
    }
    frames_new_.erase(iter->second);
    locale_new_.erase(frame_id);

    size_t kth_time = access_hist_[frame_id].front();
    k_time new_frame(frame_id, kth_time);
    auto iter2 = std::upper_bound(frames_k_.begin(), frames_k_.end(), new_frame, CmpTimeStamp);
    iter2 = frames_k_.insert(iter2, new_frame);
    locale_k_[frame_id] = iter2;
  }

  // cache list to cache list
  if (cnt >= k_) {
    auto iter = locale_k_.find(frame_id);
    if (iter == locale_k_.end()) {
      LOG_DEBUG("page is not in the cache list");
    }
    frames_k_.erase(iter->second);

    size_t kth_time = access_hist_[frame_id].front();
    k_time new_frame(frame_id, kth_time);
    auto iter2 = std::upper_bound(frames_k_.begin(), frames_k_.end(), new_frame, CmpTimeStamp);
    iter2 = frames_k_.insert(iter2, new_frame);
    locale_k_[frame_id] = iter2;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  // LOG_DEBUG("SetEvictable: %d, %d", frame_id, set_evictable);

  BUSTUB_ASSERT(replacer_size_ - frame_id > 0, "required frame_id is larger than up bound");

  if (evictable_[frame_id] == set_evictable) {
    return;
  }

  // evictable to NOT evictable. remove from lists
  if (evictable_[frame_id]) {
    evictable_[frame_id] = false;
    curr_size_--;

    if (access_cnt_[frame_id] < k_) {
      auto iter = locale_new_.find(frame_id);
      if (iter == locale_new_.end()) {
        LOG_DEBUG("page is not in the visit list");
      }
      frames_new_.erase(iter->second);
      locale_new_.erase(frame_id);
    } else {
      auto iter = locale_k_.find(frame_id);
      if (iter == locale_k_.end()) {
        LOG_DEBUG("page is not in the cache list");
      }
      frames_k_.erase(iter->second);
      locale_k_.erase(frame_id);
    }

    return;
  }

  // NOT evictable to evictable, add into list
  // if no access record, cann't be set to evictable
  if (access_cnt_[frame_id] == 0) {
    return;
  }

  evictable_[frame_id] = true;
  curr_size_++;

  if (access_cnt_[frame_id] < k_) {
    frames_new_.push_back(frame_id);
    locale_new_[frame_id] = std::prev(frames_new_.end());
  } else {
    size_t kth_time = access_hist_[frame_id].front();
    k_time new_frame(frame_id, kth_time);
    auto iter = std::upper_bound(frames_k_.begin(), frames_k_.end(), new_frame, CmpTimeStamp);
    iter = frames_k_.insert(iter, new_frame);
    locale_k_[frame_id] = iter;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // LOG_DEBUG("Remove: %d", frame_id);
  // BUSTUB_ASSERT(evictable_[frame_id], "The frame to remove is not evictable.");
  if (!evictable_[frame_id]) {
    LOG_DEBUG("The frame to remove is not evictable.");
    return;
  }

  // fresh the list
  if (access_cnt_[frame_id] >= k_) {
    auto iter = locale_k_.find(frame_id);
    if (iter == locale_k_.end()) {
      LOG_DEBUG("page is not in the cache list");
    }
    frames_k_.erase(iter->second);
    locale_k_.erase(frame_id);
  } else {
    auto iter = locale_new_.find(frame_id);
    if (iter == locale_new_.end()) {
      LOG_DEBUG("page is not in the visit list");
    }
    frames_new_.erase(iter->second);
    locale_new_.erase(frame_id);
  }

  // fresh the variables
  access_cnt_[frame_id] = 0;
  access_hist_[frame_id].clear();
  evictable_[frame_id] = false;

  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  return curr_size_;
  // LOG_DEBUG("Size = %lu", curr_size_);
}

}  // namespace bustub
