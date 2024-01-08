/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() {
  // LOG_DEBUG("default constructor called.");

  buffer_pool_manager_ = nullptr;
  page_id_ = INVALID_PAGE_ID;
  index_ = 0;
  page_ptr_ = nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, page_id_t page_id, int index) {
  // LOG_DEBUG("constructor called.");

  if (page_id_ != INVALID_PAGE_ID && buffer_pool_manager_ != nullptr) {
    buffer_pool_manager_ = buffer_pool_manager;
    page_id_ = page_id;
    index_ = index;

    LOG_DEBUG("FetchPage");
    auto ptr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id_)->GetData());
    BUSTUB_ASSERT(ptr->IsLeafPage(), "NOT LEAF PAGE.");
    page_ptr_ = static_cast<LeafPage *>(ptr);

  } else {
    page_id = INVALID_PAGE_ID;
    buffer_pool_manager_ = nullptr;
    index_ = 0;
    page_ptr_ = nullptr;
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(const IndexIterator &itr) {
  // LOG_DEBUG("copy constructor operatorcalled.");

  if (!itr.page_ptr_) {
    buffer_pool_manager_ = nullptr;
    page_id_ = INVALID_PAGE_ID;
    index_ = 0;
    page_ptr_ = nullptr;

  } else {
    buffer_pool_manager_ = itr.buffer_pool_manager_;
    page_id_ = itr.page_id_;
    index_ = itr.index_;
    page_ptr_ = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(page_id_)->GetData());
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  // LOG_DEBUG("deconstructor called.");

  if (page_ptr_) {
    buffer_pool_manager_->UnpinPage(page_id_, false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_ptr_ == nullptr; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  BUSTUB_ASSERT(page_ptr_ != nullptr, "page_ptr_ is nullptr.");
  return page_ptr_->ItemAt(index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  BUSTUB_ASSERT(page_ptr_ != nullptr, "page_ptr_ is nullptr.");
  BUSTUB_ASSERT(!IsEnd(), "iter is end.");

  if (index_ < page_ptr_->GetSize() - 1) {
    index_++;
    return *this;
  }

  page_id_t next_page_id = page_ptr_->GetNextPageId();

  // return end
  if (next_page_id == INVALID_PAGE_ID) {
    page_id_ = INVALID_PAGE_ID;
    page_ptr_ = nullptr;
    index_ = 0;
    return *this;
  }

  buffer_pool_manager_->UnpinPage(page_id_, false);
  page_id_ = next_page_id;
  page_ptr_ = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(page_id_)->GetData());
  index_ = 0;

  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  // LOG_DEBUG("operator== called.");
  if (!page_ptr_ && !itr.page_ptr_) {
    return true;
  }

  if (page_ptr_ && itr.page_ptr_) {
    return page_id_ == itr.page_id_ && index_ == itr.index_;
  }

  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool {
  // LOG_DEBUG("operator!= called.");
  return !(*this == itr);
}

INDEX_TEMPLATE_ARGUMENTS
void INDEXITERATOR_TYPE::operator=(const IndexIterator &itr) {
  // LOG_DEBUG("operator= called.");

  if (!itr.page_ptr_) {
    page_ptr_ = nullptr;
    page_id_ = INVALID_PAGE_ID;
    index_ = 0;
    buffer_pool_manager_ = nullptr;
  } else {
    buffer_pool_manager_ = itr.buffer_pool_manager_;
    page_id_ = itr.page_id_;
    index_ = itr.index_;
    page_ptr_ = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(page_id_)->GetData());
  }
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
