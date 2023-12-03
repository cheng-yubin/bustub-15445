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

#include "container/hash/extendible_hash_table.h"
#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>
#include <utility>
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size_, 0));
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

  while (!dir_[dir_index]->Insert(key, value)) {
    // the bucket is full
    if (global_depth_ == dir_[dir_index]->GetDepth()) {
      global_depth_++;

      // expand the size of direction
      int dir_size = dir_.size();
      dir_.resize(dir_size * 2);

      for (int i = 0; i < dir_size; i++) {
        dir_[dir_size + i] = dir_[i];
      }
    }
    RedistributeBucket(key);
    dir_index = IndexOf(key);
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(const K &key) -> void {
  size_t dir_index = IndexOf(key);
  int local_depth = dir_[dir_index]->GetDepth();

  // create new bucket and redistribute the tiems in old bucket
  auto bucket_new = std::make_shared<Bucket>(bucket_size_, local_depth + 1);
  auto bucket_old = dir_[dir_index];
  bucket_old->IncrementDepth();

  std::vector<std::pair<K, V>> &items_new = bucket_new->GetItems();
  std::vector<std::pair<K, V>> &items_old = bucket_old->GetItems();

  size_t index_old = dir_index & ((1 << local_depth) - 1);
  size_t index_new = index_old + (1 << local_depth);

  size_t i = 0;
  size_t &curr_size_old = bucket_old->GetCurrSize();
  size_t &curr_size_new = bucket_new->GetCurrSize();

  while (i < curr_size_old) {
    if ((std::hash<K>()(items_old[i].first) & ((1 << (local_depth + 1)) - 1)) == index_new) {
      items_new[curr_size_new].first = items_old[i].first;
      items_new[curr_size_new].second = items_old[i].second;
      ++curr_size_new;

      items_old[i].first = items_old[curr_size_old - 1].first;
      items_old[i].second = items_old[curr_size_old - 1].second;
      --curr_size_old;
    } else {
      ++i;
    }
  }

  // auto temp_iter = items_old.begin();
  // for (auto iter = items_old.begin(); iter != items_old.end();) {
  //   temp_iter = iter++;
  //   if ((std::hash<K>()(temp_iter->first) & ((1 << (local_depth + 1)) - 1)) == index_new) {
  //     items_new.splice(items_new.end(), items_old, temp_iter);
  //   }
  // }

  // redistribute the direction
  for (size_t index = 0; index < dir_.size(); index++) {
    if ((index & ((1 << (local_depth + 1)) - 1)) == index_old) {
      dir_[index] = bucket_old;
    }
    if ((index & ((1 << (local_depth + 1)) - 1)) == index_new) {
      dir_[index] = bucket_new;
    }
  }

  num_buckets_++;
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth)
    : size_(array_size), depth_(depth), curr_size_(0), vec_(array_size) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (size_t i = 0; i < curr_size_; ++i) {
    if (vec_[i].first == key) {
      value = vec_[i].second;
      return true;
    }
  }
  return false;

  // typename std::list<std::pair<K, V>>::iterator iter;
  // for (iter = list_.begin(); iter != list_.end(); iter++) {
  //   if (iter->first == key) {
  //     value = iter->second;
  //     return true;
  //   }
  // }
  // return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (size_t i = 0; i < curr_size_; ++i) {
    if (vec_[i].first == key) {
      vec_[i] = vec_[curr_size_ - 1];
      curr_size_--;
      return true;
    }
  }
  return false;

  // typename std::list<std::pair<K, V>>::iterator iter;
  // for (iter = list_.begin(); iter != list_.end(); iter++) {
  //   if (iter->first == key) {
  //     list_.erase(iter);
  //     return true;
  //   }
  // }
  // return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (size_t i = 0; i < curr_size_; ++i) {
    if (vec_[i].first == key) {
      vec_[i].second = value;
      return true;
    }
  }

  if (IsFull()) {
    return false;
  }

  vec_[curr_size_].first = key;
  vec_[curr_size_].second = value;
  ++curr_size_;
  return true;

  // typename std::list<std::pair<K, V>>::iterator iter;
  // for (iter = list_.begin(); iter != list_.end(); iter++) {
  //   if (iter->first == key) {
  //     iter->second = value;
  //     return true;
  //   }
  // }

  // if (IsFull()) {
  //   return false;
  // }

  // list_.push_back(std::pair<K, V>(key, value));
  // return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
