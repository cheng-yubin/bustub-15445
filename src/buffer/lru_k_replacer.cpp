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
    : replacer_size_(num_frames), k_(k), frame_info_(num_frames, FrameStatus(k)) {
  for (size_t i = 0; i < num_frames; ++i) {
    frame_info_[i].SetFrameID(static_cast<frame_id_t>(i));
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (curr_size_ == 0) {
    return false;
  }

  if (!frames_visit_.empty()) {
    FrameStatus *ptr = frames_visit_.front();
    *frame_id = ptr->GetFrameID();

    frames_visit_.pop_front();
    curr_size_--;

    ptr->Reset();
    return true;
  }

  if (!frames_cache_.empty()) {
    auto iter = frames_cache_.begin();
    FrameStatus *ptr = *iter;
    *frame_id = ptr->GetFrameID();

    frames_cache_.erase(iter);
    curr_size_--;

    ptr->Reset();
    return true;
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // BUSTUB_ASSERT(replacer_size_ - frame_id > 0, "required frame_id is larger than up bound");

  // the original count of access
  size_t cnt = frame_info_[frame_id].GetAccessCount();

  // If NOT evictable, no need to change lists
  if (!frame_info_[frame_id].Evictable()) {
    frame_info_[frame_id].AddRecord(curr_timestamp_++);
    return;
  }

  // visit list to visit list
  if (cnt < k_ - 1) {
    frame_info_[frame_id].AddRecord(curr_timestamp_++);

    auto iter_op = frame_info_[frame_id].GetVisitIter();
    if (!iter_op.has_value()) {
      LOG_DEBUG("page is not in the visit list");
    }
    frames_visit_.erase(iter_op.value());

    frames_visit_.emplace_back(&frame_info_[frame_id]);
    frame_info_[frame_id].SetVisitIter({std::prev(frames_visit_.end())});
  }

  // vistt list to cache list
  if (cnt == k_ - 1) {
    frame_info_[frame_id].AddRecord(curr_timestamp_++);

    auto iter_op = frame_info_[frame_id].GetVisitIter();
    if (!iter_op.has_value()) {
      LOG_DEBUG("page is not in the cache list");
    }
    frames_visit_.erase(iter_op.value());
    frame_info_[frame_id].SetVisitIter(std::nullopt);

    frames_cache_.insert(&frame_info_[frame_id]);
  }

  // cache list to cache list
  if (cnt >= k_) {
    frames_cache_.erase(&frame_info_[frame_id]);
    frame_info_[frame_id].AddRecord(curr_timestamp_++);

    frames_cache_.insert(&frame_info_[frame_id]);
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

      frames_visit_.erase(iter_op.value());
      frame_info_[frame_id].SetVisitIter(std::nullopt);

    } else {
      frames_cache_.erase(&frame_info_[frame_id]);
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
    frames_visit_.emplace_back(&frame_info_[frame_id]);
    frame_info_[frame_id].SetVisitIter({std::prev(frames_visit_.end())});

  } else {
    frames_cache_.insert(&frame_info_[frame_id]);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // LOG_DEBUG("Remove: %d", frame_id);
  // BUSTUB_ASSERT(evictable_[frame_id], "The frame to remove is not evictable.");
  if (!frame_info_[frame_id].Evictable()) {
    // LOG_DEBUG("The frame to remove is not evictable.");
    return;
  }

  // fresh the list
  if (frame_info_[frame_id].GetAccessCount() >= k_) {
    frames_cache_.erase(&frame_info_[frame_id]);

  } else {
    auto iter_op = frame_info_[frame_id].GetVisitIter();
    if (!iter_op.has_value()) {
      LOG_DEBUG("page is not in the visit list");
    }

    frames_visit_.erase(iter_op.value());
    frame_info_[frame_id].SetVisitIter(std::nullopt);
  }

  // fresh the variables
  frame_info_[frame_id].Reset();
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

FrameStatus::FrameStatus(size_t k)
    : k_(k), access_cnt_(0), evictable_(false), hist_(k, 0), curr_(0), iter_visit_op_(std::nullopt) {}

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
}

auto FrameStatus::GetAccessCount() -> size_t { return access_cnt_; }
auto FrameStatus::Evictable() -> bool { return evictable_; }
void FrameStatus::SetEvictable(bool evictable) { evictable_ = evictable; }
auto FrameStatus::GetVisitIter() -> std::optional<std::list<FrameStatus *>::iterator> { return iter_visit_op_; }
void FrameStatus::SetVisitIter(std::optional<std::list<FrameStatus *>::iterator> iter_op) { iter_visit_op_ = iter_op; }

}  // namespace bustub
