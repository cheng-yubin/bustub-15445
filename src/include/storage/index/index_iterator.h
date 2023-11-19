//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(BufferPoolManager *buffer_pool_manager, page_id_t page_id, int index);
  
  // 复制构造，深拷贝
  IndexIterator(const IndexIterator &itr);

  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool;

  auto operator!=(const IndexIterator &itr) const -> bool;

  // 赋值运算, 深拷贝
  void operator=(const IndexIterator &itr);

  void DebugInfo() const;

  inline auto GetID() -> page_id_t { return page_id_; }

 private:
  BufferPoolManager *buffer_pool_manager_;
  page_id_t page_id_;
  int index_;

  LeafPage *page_ptr_;
};

}  // namespace bustub
