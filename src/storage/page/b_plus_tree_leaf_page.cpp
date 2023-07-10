//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetLSN(INVALID_LSN);
  SetSize(0);
  SetMaxSize(max_size);
  SetParentPageId(parent_id);
  SetPageId(page_id);
  next_page_id_ = INVALID_PAGE_ID;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  // replace with your own code
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ItemAt(int index) -> MappingType & { return array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result,
                                          const KeyComparator &comparator) const -> bool {
  // linear search
  for (int index = 0; index < GetSize(); index++) {
    if (comparator(key, KeyAt(index)) == 0) {
      *result = std::vector<ValueType>{ValueAt(index)};
      return true;
    }
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertKV(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  // 叶子节点满，插入失败
  if (IsFull()) {
    LOG_DEBUG("error: array is full when InsertKV, b_plus_tree_leaf_page.cpp");
    return false;
  }

  // 已有相同的key,插入失败
  for (int index = 0; index < GetSize(); index++) {
    if (comparator(key, KeyAt(index)) == 0) {
      return false;
    }
  }

  // 元素后移，有序插入
  for (int index = GetSize() - 1; index >= 0; index--) {
    if (comparator(key, KeyAt(index)) < 0) {
      array_[index + 1] = array_[index];
    } else {
      array_[index + 1].first = key;
      array_[index + 1].second = value;
      IncreaseSize(1);
      return true;
    }
  }

  array_[0].first = key;
  array_[0].second = value;
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveKey(const KeyType &key, const KeyComparator &comparator) -> bool {
  int index = 0;
  for (; index < GetSize(); index++) {
    if (comparator(key, KeyAt(index)) == 0) {
      break;
    }
  }

  // 若没有找到对应的key，返回false
  if (index == GetSize()) {
    return false;
  }

  // 删除对应的key
  for (; index < GetSize() - 1; index++) {
    array_[index] = array_[index + 1];
  }
  IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindKey(const KeyType &key, const KeyComparator &comparator) -> int {
  int index = 0;
  for(; index < GetSize(); index++) {
    if(comparator(key, KeyAt(index)) <= 0) {
      return index;
    }
  }
  return GetSize();
}


template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
