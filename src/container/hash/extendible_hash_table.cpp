//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>
#include <memory>
#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
      dir_.push_back(std::make_shared<Bucket>(bucket_size_, 0));
      num_buckets_ = 1;
    }

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return FindInternal(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::FindInternal(const K &key, V &value) -> bool {
  size_t dir_index = IndexOf(key);
  return dir_[dir_index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return RemoveInternal(key);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RemoveInternal(const K &key) -> bool {
  size_t dir_index = IndexOf(key);
  return dir_[dir_index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  return InsertInternal(key, value);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::InsertInternal(const K &key, const V &value) {
  size_t dir_index = IndexOf(key);
  if(dir_[dir_index]->Insert(key, value)) return;

  if(global_depth_ == dir_[dir_index]->GetDepth()){
    global_depth_++;
    int dir_size = dir_.size();
    for(int i=0; i<dir_size; i++){
      dir_.push_back(dir_[i]);
    }
  }

  RedistributeBucket(dir_index);
  InsertInternal(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(size_t dir_index) -> void{
  int local_depth = dir_[dir_index]->GetDepth();

  size_t index_low = dir_index & ((1 << local_depth) - 1);
  size_t index_high = index_low + (1 << local_depth);

  auto ptr_low = std::make_shared<Bucket>(bucket_size_, local_depth+1);
  auto ptr_high = std::make_shared<Bucket>(bucket_size_, local_depth+1);
  
  std::list<std::pair<K,V>> &items = dir_[dir_index]->GetItems();
  std::list<std::pair<K,V>> &items_low = ptr_low->GetItems();
  std::list<std::pair<K,V>> &items_high = ptr_high->GetItems();
  
  for(auto iter=items.begin(); iter!=items.end(); iter++){
    if((std::hash<K>()(iter->first) & ((1 << (local_depth+1))-1)) == index_low){
      items_low.push_back(*iter);
    }else{
      items_high.push_back(*iter);
    }
  }

  dir_[index_low] = ptr_low;
  dir_[index_high] = ptr_high;

  num_buckets_++;
}


//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {  
  typename std::list<std::pair<K,V>>::iterator iter;
  for(iter=list_.begin(); iter!=list_.end(); iter++){
    if(iter->first == key){
      value = iter->second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  typename std::list<std::pair<K,V>>::iterator iter;
  for(iter=list_.begin(); iter!=list_.end(); iter++){
    if(iter->first == key){
      list_.erase(iter);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  typename std::list<std::pair<K,V>>::iterator iter;
  for(iter=list_.begin(); iter!=list_.end(); iter++){
    if(iter->first == key){
      iter->second = value;
      return true;
    }
  }
  if(IsFull()) return false;
  else{
    list_.push_back(std::pair<K,V>(key, value));
    return true;
  }
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
