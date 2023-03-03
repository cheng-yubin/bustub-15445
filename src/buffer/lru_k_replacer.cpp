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

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    std::scoped_lock<std::mutex> lock(latch_);
    if(curr_size_ == 0) return false;
    
    frame_id_t index = evict_list_.front();
    node_store_[index]->ClearRecord();
    RemoveFromEvictList(index);
    curr_size_ --;

    *frame_id = index;
    return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);

    BUSTUB_ASSERT(replacer_size_ - frame_id > 0, "required frame_id is larger than up bound");

    if(node_store_.count(frame_id) == 0){
        node_store_[frame_id] = std::make_shared<LRUKNode>(k_);
    }
    node_store_[frame_id]->RecordAccess(GetCurrentTimestamp());

    if(node_store_[frame_id]->IsEvictable()) AddToEvictList(frame_id);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::scoped_lock<std::mutex> lock(latch_);

    BUSTUB_ASSERT(replacer_size_ - frame_id > 0, "required frame_id is larger than up bound");
    BUSTUB_ASSERT(node_store_.count(frame_id) > 0, "frame id does not exist");
    
    if(node_store_[frame_id]->IsEvictable() == set_evictable) return;
    node_store_[frame_id]->SetEvictable(set_evictable);

    if(set_evictable == true){
        AddToEvictList(frame_id);
        curr_size_ ++;
    }else{
        RemoveFromEvictList(frame_id);
        curr_size_ --;
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);

    if(node_store_.count(frame_id) == 0 || node_store_[frame_id]->GetHistory().size() == 0) return;
    BUSTUB_ASSERT(node_store_[frame_id]->IsEvictable(), "The frame to remove is not evictable.");
    
    node_store_[frame_id]->ClearRecord();
    RemoveFromEvictList(frame_id);
    curr_size_ --;
}

void LRUKReplacer::AddToEvictList(frame_id_t frame_id) {
    if(evict_map_.count(frame_id) > 0) RemoveFromEvictList(frame_id);

    for(auto iter=evict_list_.begin(); iter!=evict_list_.end(); iter++){
        // when this frame should be Evicted earlier than the frame pointed by iter
        if(EvictFirst(frame_id, *iter) == true){    
            auto iter_new = evict_list_.insert(iter, frame_id);
            evict_map_[frame_id] = iter_new;
            return;
        }
    }
    auto iter_new = evict_list_.insert(evict_list_.end(), frame_id);
    evict_map_[frame_id] = iter_new;
}

auto LRUKReplacer::EvictFirst(frame_id_t frame_id1, frame_id_t frame_id2) -> bool {
    const std::list<size_t> & his1 = node_store_[frame_id1]->GetHistory();
    const std::list<size_t> & his2 = node_store_[frame_id2]->GetHistory();
    
    if(his1.size() < k_ && his2.size() >= k_) return true;
    else if(his1.size() >= k_ && his2.size() < k_) return false;
    else{
        if(his1.front() < his2.front()) return true;
        else return false;
    }
}

void LRUKReplacer::RemoveFromEvictList(frame_id_t frame_id) {
    BUSTUB_ASSERT(evict_map_.count(frame_id) == 1, "frame is not in evict_map_");
    auto iter = evict_map_[frame_id];
    evict_list_.erase(iter);
    evict_map_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

auto LRUKReplacer::GetCurrentTimestamp() -> size_t {
    struct timespec t = {0, 0};
    clock_gettime(CLOCK_REALTIME, &t);
    return (t.tv_sec*1000 + t.tv_nsec/1000000);
}


LRUKReplacer::LRUKNode::LRUKNode(size_t k) : k_(k){}

void LRUKReplacer::LRUKNode::RecordAccess(size_t timestamp) {
    // keep the latest k records.
    history_.push_back(timestamp);
    if(history_.size() > k_) history_.pop_front();
}

void LRUKReplacer::LRUKNode::SetEvictable(bool set_evictable) {
    is_evictable_ = set_evictable;
}

auto LRUKReplacer::LRUKNode::IsEvictable() -> bool {
    return is_evictable_;
}

auto LRUKReplacer::LRUKNode::GetHistory() -> std::list<size_t> & {
    return history_;
}

void LRUKReplacer::LRUKNode::ClearRecord(){
    history_.clear();
    is_evictable_ = false;
}
}  // namespace bustub
