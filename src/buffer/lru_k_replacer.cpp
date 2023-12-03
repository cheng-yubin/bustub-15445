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

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : replacer_size_(num_frames), k_(k), frame_info_(num_frames, FrameStatus(k)) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  // LOG_DEBUG("evict");
  if (curr_size_ == 0) {
    return false;
  }

  if (!frames_new_.empty()) {
    *frame_id = frames_new_.front();

    frames_new_.pop_front();
    frame_info_[*frame_id].Reset();
    curr_size_--;

    return true;
  }

  if (!frames_k_.empty()) {
    *frame_id = frames_k_.front().first;

    frames_k_.pop_front();
    frame_info_[*frame_id].Reset();
    curr_size_--;

    return true;
  }

  return false;
}

auto LRUKReplacer::CmpTimeStamp(const LRUKReplacer::k_time &t1, const LRUKReplacer::k_time &t2) -> bool {
  return t1.second < t2.second;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // BUSTUB_ASSERT(replacer_size_ - frame_id > 0, "required frame_id is larger than up bound");

  // the original count of access
  size_t cnt = frame_info_[frame_id].GetAccessCount();

  // fresh the access record
  frame_info_[frame_id].AddRecord(curr_timestamp_++);

  // If NOT evictable, no need to change lists
  if (!frame_info_[frame_id].Evictable()) {
    return;
  }

  // visit list to visit list
  if (cnt < k_ - 1) {
    auto iter_op = frame_info_[frame_id].GetVisitIter();
    if (!iter_op.has_value()) {
      LOG_DEBUG("page is not in the visit list");
    }

    frames_new_.erase(iter_op.value());
    frames_new_.emplace_back(frame_id);
    // frames_new_.push_back(frame_id);
    frame_info_[frame_id].SetVisitIter({std::prev(frames_new_.end())});
  }

  // vistt list to cache list
  if (cnt == k_ - 1) {
    auto iter_op = frame_info_[frame_id].GetVisitIter();
    if (!iter_op.has_value()) {
      LOG_DEBUG("page is not in the visit list");
    }

    frames_new_.erase(iter_op.value());
    frame_info_[frame_id].SetVisitIter(std::nullopt);

    size_t kth_time = frame_info_[frame_id].GetTimeStamp();
    k_time new_frame(frame_id, kth_time);
    auto iter2 = std::upper_bound(frames_k_.begin(), frames_k_.end(), new_frame, CmpTimeStamp);
    iter2 = frames_k_.insert(iter2, new_frame);
    frame_info_[frame_id].SetCacheIter({iter2});
  }

  // cache list to cache list
  if (cnt >= k_) {
    auto iter_op = frame_info_[frame_id].GetCacheIter();
    if (!iter_op.has_value()) {
      LOG_DEBUG("page is not in the visit list");
    }

    frames_k_.erase(iter_op.value());

    size_t kth_time = frame_info_[frame_id].GetTimeStamp();
    k_time new_frame(frame_id, kth_time);
    auto iter2 = std::upper_bound(frames_k_.begin(), frames_k_.end(), new_frame, CmpTimeStamp);
    iter2 = frames_k_.insert(iter2, new_frame);
    frame_info_[frame_id].SetCacheIter({iter2});
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  // LOG_DEBUG("SetEvictable: %d, %d", frame_id, set_evictable);
  // BUSTUB_ASSERT(replacer_size_ - frame_id > 0, "required frame_id is larger than up bound");

  if (frame_info_[frame_id].Evictable() == set_evictable) {
    return;
  }

  // evictable to NOT evictable. remove from lists
  if (frame_info_[frame_id].Evictable()) {
    frame_info_[frame_id].SetEvictable(false);
    curr_size_--;

    if (frame_info_[frame_id].GetAccessCount() < k_) {
      auto iter_op = frame_info_[frame_id].GetVisitIter();
      if (!iter_op.has_value()) {
        LOG_DEBUG("page is not in the visit list");
      }

      frames_new_.erase(iter_op.value());
      frame_info_[frame_id].SetVisitIter(std::nullopt);

    } else {
      auto iter_op = frame_info_[frame_id].GetCacheIter();
      if (!iter_op.has_value()) {
        LOG_DEBUG("page is not in the cache list");
      }

      frames_k_.erase(iter_op.value());
      frame_info_[frame_id].SetCacheIter(std::nullopt);
    }

    return;
  }

  // NOT evictable to evictable, add into list
  // if no access record, cann't be set to evictable
  if (frame_info_[frame_id].GetAccessCount() == 0) {
    return;
  }

  frame_info_[frame_id].SetEvictable(true);
  curr_size_++;

  if (frame_info_[frame_id].GetAccessCount() < k_) {
    // frames_new_.push_back(frame_id);
    frames_new_.emplace_back(frame_id);
    frame_info_[frame_id].SetVisitIter({std::prev(frames_new_.end())});

  } else {
    size_t kth_time = frame_info_[frame_id].GetTimeStamp();
    k_time new_frame(frame_id, kth_time);
    auto iter = std::upper_bound(frames_k_.begin(), frames_k_.end(), new_frame, CmpTimeStamp);
    iter = frames_k_.insert(iter, new_frame);
    frame_info_[frame_id].SetCacheIter({iter});
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // LOG_DEBUG("Remove: %d", frame_id);
  // BUSTUB_ASSERT(evictable_[frame_id], "The frame to remove is not evictable.");
  if (!frame_info_[frame_id].Evictable()) {
    LOG_DEBUG("The frame to remove is not evictable.");
    return;
  }

  // fresh the list
  if (frame_info_[frame_id].GetAccessCount() >= k_) {
    auto iter_op = frame_info_[frame_id].GetCacheIter();
    if (!iter_op.has_value()) {
      LOG_DEBUG("page is not in the cache list");
    }

    frames_k_.erase(iter_op.value());
    frame_info_[frame_id].SetCacheIter(std::nullopt);

  } else {
    auto iter_op = frame_info_[frame_id].GetVisitIter();
    if (!iter_op.has_value()) {
      LOG_DEBUG("page is not in the visit list");
    }

    frames_new_.erase(iter_op.value());
    frame_info_[frame_id].SetVisitIter(std::nullopt);
  }

  // fresh the variables
  frame_info_[frame_id].Reset();
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

FrameStatus::FrameStatus(size_t k)
    : k_(k),
      access_cnt_(0),
      evictable_(false),
      hist_(k, 0),
      curr_(0),
      iter_visit_op_(std::nullopt),
      iter_cache_op_(std::nullopt) {}

void FrameStatus::AddRecord(size_t curr_timestamp) {
  if (access_cnt_ < k_) {
    hist_[access_cnt_++] = curr_timestamp;
  } else {
    hist_[curr_] = curr_timestamp;
    curr_ = (curr_ + 1) % k_;
  }
}

auto FrameStatus::GetTimeStamp() -> size_t { return hist_[curr_]; }

void FrameStatus::Reset() {
  access_cnt_ = 0;
  evictable_ = false;
  curr_ = 0;

  iter_visit_op_ = std::nullopt;
  iter_cache_op_ = std::nullopt;
}

auto FrameStatus::GetAccessCount() -> size_t { return access_cnt_; }

auto FrameStatus::Evictable() -> bool { return evictable_; }

void FrameStatus::SetEvictable(bool evictable) { evictable_ = evictable; }

auto FrameStatus::GetVisitIter() -> std::optional<std::list<frame_id_t>::iterator> { return iter_visit_op_; }

auto FrameStatus::GetCacheIter() -> std::optional<std::list<k_time>::iterator> { return iter_cache_op_; }

void FrameStatus::SetVisitIter(std::optional<std::list<frame_id_t>::iterator> iter_op) { iter_visit_op_ = iter_op; }

void FrameStatus::SetCacheIter(std::optional<std::list<k_time>::iterator> iter_op) { iter_cache_op_ = iter_op; }
}  // namespace bustub
