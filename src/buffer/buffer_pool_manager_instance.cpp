//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else if (!replacer_->Evict(&frame_id)) {
    return nullptr;
  }

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  page_id_t page_index = AllocatePage();

  if (pages_[frame_id].IsDirty()) {
    FlushPgImp(pages_[frame_id].page_id_);
  }

  if (pages_[frame_id].page_id_ != INVALID_PAGE_ID) {
    page_table_->Remove(pages_[frame_id].page_id_);
  }
  page_table_->Insert(page_index, frame_id);

  pages_[frame_id].page_id_ = page_index;
  pages_[frame_id].ResetMemory();
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;

  *page_id = page_index;
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    pages_[frame_id].pin_count_++;

    return &pages_[frame_id];
  }

  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else if (!replacer_->Evict(&frame_id)) {
    return nullptr;
  }

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  if (pages_[frame_id].IsDirty()) {
    FlushPgImp(pages_[frame_id].page_id_);
  }

  if (pages_[frame_id].page_id_ != INVALID_PAGE_ID) {
    page_table_->Remove(pages_[frame_id].page_id_);
  }
  page_table_->Insert(page_id, frame_id);

  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].ResetMemory();
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;

  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id) || pages_[frame_id].pin_count_ <= 0) {
    return false;
  }

  pages_[frame_id].is_dirty_ = pages_[frame_id].is_dirty_ || is_dirty;
  pages_[frame_id].pin_count_--;

  if (pages_[frame_id].pin_count_ <= 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  pages_[frame_id].is_dirty_ = false;

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);

  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ != INVALID_PAGE_ID) {
      FlushPgImp(pages_[i].page_id_);
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }

  replacer_->Remove(frame_id);
  page_table_->Remove(page_id);
  DeallocatePage(page_id);

  pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  free_list_.push_back(frame_id);

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
