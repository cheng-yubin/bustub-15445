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
    buffer_pool_manager_ = buffer_pool_manager;
    page_id_ = page_id;
    index_ = index;
    page_ptr_ = nullptr;

    if(page_id_ != INVALID_PAGE_ID && buffer_pool_manager_ != nullptr) {
        // LOG_DEBUG("FetchPage");
        auto ptr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id_)->GetData());
        if(ptr->IsLeafPage()) {
            // LOG_DEBUG("asign page_ptr_");
            page_ptr_ = static_cast<LeafPage *>(ptr);
        } else {
            buffer_pool_manager->UnpinPage(ptr->GetPageId(), false);
        }
    }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(const IndexIterator &itr) {
    // LOG_DEBUG("copy constructor called.");
    buffer_pool_manager_ = itr.buffer_pool_manager_;
    page_id_ = itr.page_id_;
    index_ = itr.index_;

    if(!itr.page_ptr_) {
        page_ptr_ = nullptr;
    } else {
        page_ptr_ = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(page_id_)->GetData());
    }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator(){
    // LOG_DEBUG("deconstructor called.");
    debug_info();
    if(page_ptr_ != nullptr) {
        // LOG_DEBUG("unpin.");
        buffer_pool_manager_->FetchPage(page_id_)->GetData();
        // buffer_pool_manager_->UnpinPage(INVALID_PAGE_ID, false);
    } else {
        // LOG_DEBUG("not unpin");
    }
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
    BUSTUB_ASSERT(page_ptr_ != nullptr, "page_ptr_ is nullptr.");
    return (page_ptr_->GetNextPageId() == INVALID_PAGE_ID) && (index_ == page_ptr_->GetSize());
 }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { 
    BUSTUB_ASSERT(page_ptr_ != nullptr, "page_ptr_ is nullptr.");
    return page_ptr_->ItemAt(index_);
 }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & { 
    BUSTUB_ASSERT(page_ptr_ != nullptr, "page_ptr_ is nullptr.");
    BUSTUB_ASSERT(!IsEnd(), "iter is end.");
    
    if(index_ < page_ptr_->GetSize() - 1) {
        index_++;
        return *this;
    }
    
    page_id_t next_page_id = page_ptr_->GetNextPageId();
    
    // 超尾
    if(next_page_id == INVALID_PAGE_ID) {
        index_++;
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
    BUSTUB_ASSERT(page_ptr_ != nullptr, "page_ptr_ is nullptr.");
    BUSTUB_ASSERT(itr.page_ptr_ != nullptr, "itr.page_ptr_ is nullptr.");

    return page_id_ == itr.page_id_ && index_ == itr.index_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool {
    // LOG_DEBUG("operator!= called.");
    BUSTUB_ASSERT(page_ptr_ != nullptr, "page_ptr_ is nullptr.");
    BUSTUB_ASSERT(itr.page_ptr_ != nullptr, "itr.page_ptr_ is nullptr.");

    return page_id_ != itr.page_id_ || index_ != itr.index_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator=(const IndexIterator &itr) {
    // LOG_DEBUG("operator= called.");
    buffer_pool_manager_ = itr.buffer_pool_manager_;
    page_id_ = itr.page_id_;
    index_ = itr.index_;

    if(itr.page_ptr_ != nullptr) {
        page_ptr_ = nullptr;
    } else {
        page_ptr_ = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(page_id_)->GetData());
    }
}

INDEX_TEMPLATE_ARGUMENTS
void INDEXITERATOR_TYPE::debug_info() const {
    // LOG_DEBUG("page_id = %d", page_id_);
    // LOG_DEBUG("index = %d", index_);
    // LOG_DEBUG("ptr is null? %d", page_ptr_==nullptr);
    // LOG_DEBUG("buffer_ptr is null? %d", buffer_pool_manager_==nullptr);
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
