//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  LOG_DEBUG("leaf page init");
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetLSN(INVALID_LSN);
  SetSize(0);
  SetMaxSize(max_size);
  SetParentPageId(parent_id);
  SetPageId(page_id);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { 
  return array_[index].second; 
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  array_[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ItemAt(int index) -> MappingType& {
  return array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetValue(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  ValueType value = ValueAt(GetSize() - 1);
  for(int index = 1; index < GetSize(); index++) {
    if(comparator(key, KeyAt(index)) < 0) {
      value = ValueAt(index - 1);
      break;
    }
  }
  return value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindValue(const ValueType &value) -> int {
  for(int i = 0; i < GetSize(); i++) {
    if(ValueAt(i) == value) {
      return i;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertKV(const KeyType &key, const ValueType &value, const KeyComparator &comparator) -> bool {
  if(IsFull()) {
    LOG_DEBUG("error: array is full when InsertKV, b_plus_tree_leaf_page.cpp");
    return false;
  }

  for(int index = GetSize() - 1; index >= 1; index--) {
    LOG_DEBUG("index = %d \n", index);

    if(comparator(key, KeyAt(index)) < 0) {
      array_[index + 1] = array_[index];
    }
    else {
      array_[index + 1].first = key;
      array_[index + 1].second = value;
      IncreaseSize(1);
      return true;
    }
  }
  
  LOG_DEBUG("Insert to array[0]");
  array_[1].first = key;
  array_[1].second = value;
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetSibling(const ValueType &value, ValueType &left_sibling, ValueType &right_sibling, const KeyComparator &comparator) {
  int index = 0;
  for(; index < GetSize(); index++) {
    if(value == ValueAt(index)) {
      break;
    }
  }
  
  left_sibling = index > 0 ? ValueAt(index - 1) : INVALID_PAGE_ID;
  right_sibling = index < (GetSize() - 1) ? ValueAt(index + 1) : INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveKV(const int index) -> bool {
  if (index >= GetSize()) {
    LOG_DEBUG("index overflow.");
    return false;
  }

  for (int i = index; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  } 
  IncreaseSize(-1);
  return true;
}



// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
