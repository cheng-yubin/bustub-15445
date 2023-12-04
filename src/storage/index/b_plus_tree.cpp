#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  LOG_DEBUG("leaf_max_size: %d", leaf_max_size);
  LOG_DEBUG("internal_max_size: %d", internal_max_size);
}

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::~BPlusTree() = default;

/*
 * Helper function to decide whether current b+tree is empty
 * 判空，不需要加锁
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  bool isempty = (root_page_id_ == INVALID_PAGE_ID);
  return isempty;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  LOG_DEBUG("getvalue start. key = %ld", key.ToString());
  Page *raw_page = nullptr;
  LeafPage *l_page = nullptr;
  bool get_page =
      GetLeafPageOptimistic(key, &raw_page, &l_page, transaction, false);  // 读模式获取leaf_page和对应rlatch

  if (!get_page) {
    BUSTUB_ASSERT(raw_page == nullptr, "NEED TO UNPIN");
    return false;
  }

  bool get_value = l_page->GetValue(key, result, comparator_);

  raw_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(l_page->GetPageId(), false);

  // LOG_DEBUG("getvalue end \n");
  return get_value;
}

/*
 * 以乐观锁形式，根据key获取对应的叶子节点
 * 获取失败返回false，获取成功返回true,fetch该节点，并根据rw_option选择rlatch / wlatch
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPageOptimistic(const KeyType &key, Page **raw_page_pptr, LeafPage **leaf_page_pptr,
                                           Transaction *transaction, bool rw_option) -> bool {
  // 树为空，获取叶子节点失败
  if (IsEmpty()) {
    return false;
  }

  // 获取根节点，需要循环检查root_page_id, 因为wlatch等待过程中，其他线程可能更新了根节点，n_page = 1
  page_id_t page_id;
  Page *raw_page;
  BPlusTreePage *page;
  while (true) {
    page_id = root_page_id_;
    raw_page = buffer_pool_manager_->FetchPage(page_id);            // 存储页
    page = reinterpret_cast<BPlusTreePage *>(raw_page->GetData());  // 内容页

    if (page->IsLeafPage() && rw_option) {  // 根节点可能是叶子节点
      raw_page->WLatch();
    } else {
      raw_page->RLatch();
    }

    if (root_page_id_ == page_id) {  // 成功获取根节点
      break;
    }

    // 等待获取latch过程中，根节点已经发生改变，需要解锁并重新获取
    if (page->IsLeafPage() && rw_option) {
      raw_page->WUnlatch();
    } else {
      raw_page->RUnlatch();
    }
    buffer_pool_manager_->UnpinPage(page_id, false);
  }

  // 逐层向下获取节点,获取子结点latch后释放父节点latch,乐观假设叶子节点是安全的
  while (true) {
    // 内部节点
    if (page->IsRootPage()) {
      auto r_page = reinterpret_cast<InternalPage *>(page);

      // 获取子结点
      page_id_t page_id_child = r_page->GetValue(key, comparator_);
      auto raw_page_child = buffer_pool_manager_->FetchPage(page_id_child);  // 存储叶
      page = reinterpret_cast<BPlusTreePage *>(raw_page_child->GetData());

      // 子结点latch, 页面类型确定后不会改变，因此判断类型不需要上锁。此时仍然持有父节点latch
      if (page->IsLeafPage() && rw_option) {
        raw_page_child->WLatch();
      } else {
        raw_page_child->RLatch();
      }

      // 释放父节点，先unlatch,再unpin，避免unpin后该page被替换
      raw_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page_id, false);

      // 更新变量
      page_id = page_id_child;
      raw_page = raw_page_child;

    } else if (page->IsLeafPage()) {
      auto l_page = reinterpret_cast<LeafPage *>(page);

      *raw_page_pptr = raw_page;
      *leaf_page_pptr = l_page;

      // 返回，此时持有叶子节点的latch，类型由rw_option决定
      return true;

    } else {
      BUSTUB_ASSERT(false, "INVALID PAGE TYPE");
      return false;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CheckSafe(BPlusTreePage *page_ptr, Operat op) -> bool {
  // 对于内部节点和插入操作，不满即是安全的
  if (page_ptr->IsRootPage() && op == INSERT) {
    return (!page_ptr->IsFull());
  }

  // 对于叶子节点和插入操作，插入后仍然不满是安全的
  if (page_ptr->IsLeafPage() && op == INSERT) {
    return page_ptr->GetSize() < page_ptr->GetMaxSize() - 1;
  }

  // 对于删除操作，大于半满是安全的
  if (op == REMOVE) {
    return (page_ptr->GetSize() > page_ptr->GetMinSize());
  }

  BUSTUB_ASSERT(false, "INVALID TYPE");
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPagePessimistic(const KeyType &key, Page **raw_page_pptr, LeafPage **leaf_page_pptr,
                                            Transaction *transaction, Operat op, WlatchVector &pages_wlatch) -> bool {
  // 暂存被写模式锁定的页
  pages_wlatch.clear();

  // 树为空，获取叶子节点失败
  if (IsEmpty()) {
    return false;
  }

  // 获取根节点，需要循环检查root_page_id, 因为wlatch等待过程中，其他线程可能更新了根节点
  page_id_t page_id;
  Page *raw_page;
  while (true) {
    page_id = root_page_id_;
    raw_page = buffer_pool_manager_->FetchPage(page_id);  // 存储页
    raw_page->WLatch();

    // 成功获取根节点
    if (root_page_id_ == page_id) {
      // LOG_DEBUG("GetLeafPagePessimistic: key = %ld get root page id = %d success.", key.ToString(), page_id);
      break;
    }
    // 根节点已经发生改变
    raw_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page_id, false);
    // LOG_DEBUG("GetLeafPagePessimistic: key = %ld  root page id has changed", key.ToString());
  }

  auto page = reinterpret_cast<BPlusTreePage *>(raw_page->GetData());  // 内容页

  while (true) {
    // 检查page是否安全
    if (CheckSafe(page, op)) {
      // 解锁之前的page
      for (auto ptrs : pages_wlatch) {
        ptrs.first->WUnlatch();
        buffer_pool_manager_->UnpinPage(ptrs.second->GetPageId(), false);  // unpin
      }
      pages_wlatch.clear();
    }

    // 加入该page
    pages_wlatch.push_back(std::pair<Page *, BPlusTreePage *>(raw_page, page));

    if (page->IsRootPage()) {  // 内部节点
      auto r_page = reinterpret_cast<InternalPage *>(page);

      // 获取子结点
      page_id_t page_id_child = r_page->GetValue(key, comparator_);
      auto raw_page_child = buffer_pool_manager_->FetchPage(page_id_child);  // 存储叶
      page = reinterpret_cast<BPlusTreePage *>(raw_page_child->GetData());

      // 子结点latch
      BUSTUB_ASSERT(raw_page_child != nullptr, "raw_page_child nullptr!");
      raw_page_child->WLatch();

      // 更新变量
      page_id = page_id_child;
      raw_page = raw_page_child;

    } else if (page->IsLeafPage()) {
      auto l_page = reinterpret_cast<LeafPage *>(page);

      *raw_page_pptr = raw_page;
      *leaf_page_pptr = l_page;

      return true;

    } else {
      BUSTUB_ASSERT(false, "INVALID_INDEX_PAGE");
      return false;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CreateTree() {
  root_mutex_.lock();

  // 再次检查是否为空，防止在等待获取mutex时，已有其他线程创建了树
  if (!IsEmpty()) {
    root_mutex_.unlock();
    return;
  }
  // LOG_DEBUG("Tree is empty, create a tree");

  // 创建叶子节点作为根节点
  page_id_t root_page_temp;
  Page *raw_root_page = buffer_pool_manager_->NewPage(&root_page_temp);
  auto leaf_page = reinterpret_cast<LeafPage *>(raw_root_page->GetData());
  leaf_page->Init(root_page_temp, INVALID_PAGE_ID, leaf_max_size_);

  // 释放page,需要写入
  buffer_pool_manager_->UnpinPage(root_page_temp, true);

  // 新增root_page_id注册
  root_page_id_ = root_page_temp;
  UpdateRootPageId(1);

  root_mutex_.unlock();

  // LOG_DEBUG("create tree end. \n");
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // buffer_pool_manager_->CheckLatch();
  LOG_DEBUG("insert start. key = %ld", key.ToString());

  // 树空，创建树
  if (IsEmpty()) {
    CreateTree();
  }

  // 乐观锁形式查找，根据key找到对应的leaf_page，并获取写wlatch
  LeafPage *l_page = nullptr;
  Page *raw_page = nullptr;
  // LOG_DEBUG("GetLeafPageOptimistic start key = %ld", key.ToString());
  bool get_page = GetLeafPageOptimistic(key, &raw_page, &l_page, transaction, true);
  // LOG_DEBUG("GetLeafPageOptimistic end key = %ld", key.ToString());

  if (!get_page) {
    BUSTUB_ASSERT(raw_page == nullptr, "NO NEED TO UNPIN");
    return false;
  }

  // 判断能够直接插入而不分裂，即插入后仍然不满
  if (l_page->GetSize() < l_page->GetMaxSize() - 1) {
    // 插入数据, 若key重复，插入失败
    bool insert = l_page->InsertKV(key, value, comparator_);

    // 解锁
    raw_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(l_page->GetPageId(), true);

    // LOG_DEBUG("insert end. key = %ld \n", key.ToString());
    return insert;
  }

  // 解锁
  raw_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(l_page->GetPageId(), false);

  // 插入后需要分裂，悲观锁形式查找对应leaf_page
  WlatchVector pages_wlatch;

  raw_page = nullptr;
  l_page = nullptr;
  // LOG_DEBUG("GetLeafPagePessimistic start key = %ld", key.ToString());
  get_page = GetLeafPagePessimistic(key, &raw_page, &l_page, transaction, INSERT, pages_wlatch);  // 写模式latch
  // LOG_DEBUG("GetLeafPagePessimistic end key = %ld", key.ToString());

  if (!get_page) {
    BUSTUB_ASSERT(l_page == nullptr, "l_page exist");

    for (auto ptrs : pages_wlatch) {
      ptrs.first->WUnlatch();                                            // unlatch
      buffer_pool_manager_->UnpinPage(ptrs.second->GetPageId(), false);  // unpin
    }
    // LOG_DEBUG("insert end. key = %ld", key.ToString());
    return false;
  }

  // 插入，若key重复，插入失败
  bool insert_kv = l_page->InsertKV(key, value, comparator_);

  // 插入成功后叶子节点满，需要分裂
  if (l_page->IsFull()) {
    // LOG_DEBUG("leaf page is full, start to split.");
    SplitPage(l_page->GetPageId());
    // LOG_DEBUG("split end.");
  } else {
    // LOG_DEBUG("not need to split, size = %d", l_page->GetSize());
  }

  for (auto ptrs : pages_wlatch) {
    ptrs.first->WUnlatch();                                           // unlatch
    buffer_pool_manager_->UnpinPage(ptrs.second->GetPageId(), true);  // unpin
  }

  // LOG_DEBUG("insert end. key = %ld \n", key.ToString());
  return insert_kv;
}

// 分裂叶子节点，由于所有可能涉及到的已有节点均已经获取wlatch，而新创建节点不需要锁定，因此不需要考虑锁的问题
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitPage(page_id_t page_id) {
  auto page_ptr = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());

  // 新的叶子节点
  page_id_t new_leaf_page_id;
  auto new_leaf_page_ptr = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&new_leaf_page_id)->GetData());
  new_leaf_page_ptr->Init(new_leaf_page_id, INVALID_PAGE_ID, leaf_max_size_);

  // 分配叶子节点kv
  int min_size = page_ptr->GetMinSize();
  int size = page_ptr->GetSize();
  for (int i = 0; i < size - min_size; i++) {
    new_leaf_page_ptr->ItemAt(i) = page_ptr->ItemAt(min_size + i);
  }
  page_ptr->SetSize(min_size);
  new_leaf_page_ptr->SetSize(size - min_size);

  // 设置兄弟节点
  new_leaf_page_ptr->SetNextPageId(page_ptr->GetNextPageId());
  page_ptr->SetNextPageId(new_leaf_page_id);

  // 回溯修改内部节点
  page_id_t parent_page_id = page_ptr->GetParentPageId();  // 父节点
  KeyType key = new_leaf_page_ptr->KeyAt(0);

  BPlusTreePage *old_child_ptr = page_ptr;
  BPlusTreePage *child_page_ptr = new_leaf_page_ptr;  // 新创建的节点

  InternalPage *parent_page_ptr;

  while (true) {
    // 根节点满，需要增加一层，更新根节点,此时必然拥有根节点的wlatch,可以进行根节点更新
    if (parent_page_id == INVALID_PAGE_ID) {
      // LOG_DEBUG("create new root page");
      // 新创建一个根节点并初始化
      page_id_t root_page_temp;
      Page *raw_root = buffer_pool_manager_->NewPage(&root_page_temp);

      parent_page_ptr = reinterpret_cast<InternalPage *>(raw_root->GetData());
      parent_page_ptr->Init(root_page_temp, INVALID_PAGE_ID, internal_max_size_);

      // 设置KV
      parent_page_ptr->SetKeyAt(1, key);
      parent_page_ptr->SetValueAt(0, old_child_ptr->GetPageId());
      parent_page_ptr->SetValueAt(1, child_page_ptr->GetPageId());
      parent_page_ptr->SetSize(2);

      // 设置父节点
      old_child_ptr->SetParentPageId(root_page_temp);
      child_page_ptr->SetParentPageId(root_page_temp);

      // 对page解除锁定
      buffer_pool_manager_->UnpinPage(old_child_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(root_page_temp, true);
      buffer_pool_manager_->UnpinPage(child_page_ptr->GetPageId(), true);

      // 更新根节点
      root_page_id_ = root_page_temp;
      UpdateRootPageId(0);

      // 退出循环
      break;
    }

    // 非根节点
    parent_page_ptr = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());

    // 父节点未满，可直接插入
    if (!parent_page_ptr->IsFull()) {
      // LOG_DEBUG("parent page is not full");
      // 向父节点插入KV, 设置size
      parent_page_ptr->InsertKV(key, child_page_ptr->GetPageId(), comparator_);

      // 设置父节点
      child_page_ptr->SetParentPageId(parent_page_id);

      // 对page解除锁定
      buffer_pool_manager_->UnpinPage(old_child_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_page_id, true);
      buffer_pool_manager_->UnpinPage(child_page_ptr->GetPageId(), true);

      // 退出循环
      break;
    }

    // 父节点满，需要分裂
    // LOG_DEBUG("parent page is full");
    // 新的内部节点，即父节点的兄弟节点
    page_id_t new_inter_page_id;
    auto new_inter_page_ptr =
        reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&new_inter_page_id)->GetData());
    new_inter_page_ptr->Init(new_inter_page_id, INVALID_PAGE_ID, internal_max_size_);

    // 划分KV
    int mid_size = (parent_page_ptr->GetSize() + 1) / 2;
    int max_size = parent_page_ptr->GetMaxSize();

    if (comparator_(key, parent_page_ptr->KeyAt(max_size - 1)) > 0) {  // key大于最后一个
      new_inter_page_ptr->ItemAt(mid_size - 1) =
          std::pair<KeyType, page_id_t>(key, child_page_ptr->GetPageId());  // 插入key
    } else {
      new_inter_page_ptr->ItemAt(mid_size - 1) = parent_page_ptr->ItemAt(max_size - 1);
      parent_page_ptr->IncreaseSize(-1);
      parent_page_ptr->InsertKV(key, child_page_ptr->GetPageId(), comparator_);  // 父节点中插入key,使之有序
    }

    for (int i = 0; i < mid_size - 1; i++) {
      new_inter_page_ptr->ItemAt(i) = parent_page_ptr->ItemAt(max_size - mid_size + 1 + i);
    }

    parent_page_ptr->SetSize(max_size - mid_size + 1);
    new_inter_page_ptr->SetSize(mid_size);

    // 父节点临时设置为parent_page_id，统一处理
    child_page_ptr->SetParentPageId(parent_page_id);

    // 子结点完成设置，可以解除锁定
    buffer_pool_manager_->UnpinPage(old_child_ptr->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(child_page_ptr->GetPageId(), true);

    // 新节点的子结点重新设置parenetID
    for (int i = 0; i < mid_size; i++) {
      Page *raw_ptr = buffer_pool_manager_->FetchPage(new_inter_page_ptr->ValueAt(i));
      auto temp_ptr = reinterpret_cast<InternalPage *>(raw_ptr->GetData());
      if (temp_ptr->GetPageId() != old_child_ptr->GetPageId()) {
        raw_ptr->WLatch();
      }
      temp_ptr->SetParentPageId(new_inter_page_id);

      if (temp_ptr->GetPageId() != old_child_ptr->GetPageId()) {
        raw_ptr->WUnlatch();
      }
      buffer_pool_manager_->UnpinPage(new_inter_page_ptr->ValueAt(i), true);
    }

    // 更新状态量，进入更上层循环
    old_child_ptr = parent_page_ptr;                      // 原父节点
    parent_page_id = parent_page_ptr->GetParentPageId();  // 上一层父节点
    key = new_inter_page_ptr->KeyAt(0);
    child_page_ptr = new_inter_page_ptr;  // 新节点
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // buffer_pool_manager_->CheckLatch();
  LOG_DEBUG("remove start. key = %ld", key.ToString());
  // 树为空，返回
  if (IsEmpty()) {
    LOG_DEBUG("remove end. key = %ld \n", key.ToString());
    return;
  }

  // 乐观锁形式查找，根据key找到对应的leaf_page，并获取写wlatch
  LeafPage *l_page;
  Page *raw_page;
  LOG_DEBUG("GetLeafPageOptimistic start key = %ld", key.ToString());
  bool get_page = GetLeafPageOptimistic(key, &raw_page, &l_page, transaction, true);
  LOG_DEBUG("GetLeafPageOptimistic end key = %ld", key.ToString());

  if (!get_page) {
    LOG_DEBUG("remove end. key = %ld \n", key.ToString());
    return;
  }

  // 判断能够直接删除而不合并，即删除后仍然半满，或者该叶子节点为根节点
  if (l_page->GetSize() > l_page->GetMinSize() || l_page->GetParentPageId() == INVALID_PAGE_ID) {
    // 删除数据
    l_page->RemoveKey(key, comparator_);

    // 解锁
    raw_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(l_page->GetPageId(), true);

    // 若没有找到对应key,删除失败
    LOG_DEBUG("remove end. key = %ld \n", key.ToString());
    return;
  }

  // 解锁
  raw_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(l_page->GetPageId(), false);

  // 删除后需要借调或合并，悲观锁形式查找对应leaf_page
  WlatchVector pages_wlatch;
  LOG_DEBUG("GetLeafPagePessimistic start key = %ld", key.ToString());
  get_page = GetLeafPagePessimistic(key, &raw_page, &l_page, transaction, REMOVE, pages_wlatch);  // 写模式latch
  LOG_DEBUG("GetLeafPagePessimistic end key = %ld", key.ToString());

  if (!get_page) {
    for (auto ptrs : pages_wlatch) {
      ptrs.first->WUnlatch();                                            // unlatch
      buffer_pool_manager_->UnpinPage(ptrs.second->GetPageId(), false);  // unpin
    }
    LOG_DEBUG("remove end. key = %ld \n", key.ToString());
    return;
  }

  // // 输出锁定的页id
  // LOG_DEBUG("remove: GetLeafPagePessimistic ****************************************************");
  // for (auto ptrs : pages_wlatch) {
  //   LOG_DEBUG("wlatch: %d", ptrs.second->GetPageId());
  // }
  // LOG_DEBUG("***********************************************************************************");

  // 删除KV
  l_page->RemoveKey(key, comparator_);

  // 检查是否需要合并
  if (l_page->Downflow()) {
    LOG_DEBUG("Need to Redistribution. key = %ld", key.ToString());
    RedistributePage(l_page->GetPageId(), pages_wlatch);
    LOG_DEBUG("redistribution end. key = %ld", key.ToString());
  } else {
    LOG_DEBUG("No need to Redistribution. key = %ld", key.ToString());
  }

  // 解锁
  for (auto ptrs : pages_wlatch) {
    ptrs.first->WUnlatch();                                           // unlatch
    buffer_pool_manager_->UnpinPage(ptrs.second->GetPageId(), true);  // unpin
  }
  LOG_DEBUG("remove end. key = %ld \n", key.ToString());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RedistributePage(page_id_t page_id, WlatchVector &pages_wlatch) {
  auto page_ptr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());

  page_id_t parent_page_id = page_ptr->GetParentPageId();
  InternalPage *parent_page_ptr = nullptr;

  while (true) {
    // 是根节点，允许下溢，不需要再处理
    if (parent_page_id == INVALID_PAGE_ID) {
      LOG_DEBUG("parent is root page.");

      buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
      break;
    }

    // 获取父节点，已有wlatch
    parent_page_ptr = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());

    // 从兄弟节点借，不会导致更上层变更
    LOG_DEBUG("start to borrow, page id = %d", page_ptr->GetPageId());
    bool borrow = BorrowFromSibling(page_ptr, parent_page_ptr, pages_wlatch);
    if (borrow) {
      LOG_DEBUG("borrow from sibling succuss.");
      buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_page_ptr->GetPageId(), true);
      break;
    }

    // 无法借到，合并节点
    LOG_DEBUG("start to merge.");
    Merge(page_ptr, parent_page_ptr, pages_wlatch);

    // 判断合并后父节点状态
    int parent_size = parent_page_ptr->GetSize();

    // 父节点是根节点且父节点只有一个子节点，树可以降低一层
    if (parent_size == 1 && parent_page_ptr->GetParentPageId() == INVALID_PAGE_ID) {
      LOG_DEBUG("need to lower the tree. new root page id = %d", parent_page_ptr->ValueAt(0));

      // 修改root节点
      page_id_t p_id = parent_page_ptr->ValueAt(0);

      // 不需要获取wlatch，因为在合并过程中已经获取过，而其他线程由于父节点的wlatch阻挡，不可能获取wlatch
      Page *raw_ptr = buffer_pool_manager_->FetchPage(p_id);
      auto ptr = reinterpret_cast<BPlusTreePage *>(raw_ptr->GetData());

      ptr->SetParentPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(p_id, true);

      root_page_id_ = p_id;
      UpdateRootPageId(0);

      // 解除锁定
      buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_page_ptr->GetPageId(), true);
      break;
    }

    // 需要继续合并
    if (parent_size < parent_page_ptr->GetMinSize()) {
      LOG_DEBUG("need to continue to merge.");
      buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), true);

      // 修改状态变量
      page_ptr = parent_page_ptr;
      parent_page_id = page_ptr->GetParentPageId();

    } else {
      // 解除锁定
      buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_page_ptr->GetPageId(), true);
      break;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BorrowFromSibling(BPlusTreePage *page_ptr, InternalPage *parent_page_ptr,
                                       WlatchVector &pages_wlatch) -> bool {
  page_id_t left_sibling_id;
  page_id_t right_sibling_id;
  BPlusTreePage *left_sibling_ptr;
  BPlusTreePage *right_sibling_ptr;

  // 获取左右兄弟节点的id
  parent_page_ptr->GetSibling(page_ptr->GetPageId(), left_sibling_id, right_sibling_id, comparator_);

  // 有左兄弟
  if (left_sibling_id != INVALID_PAGE_ID) {
    LOG_DEBUG("borrow from left sibling, page id = %d", left_sibling_id);
    // 获取左兄弟节点和wlatch
    Page *raw_left_ptr = buffer_pool_manager_->FetchPage(left_sibling_id);
    raw_left_ptr->WLatch();

    left_sibling_ptr = reinterpret_cast<BPlusTreePage *>(raw_left_ptr->GetData());
    int left_size = left_sibling_ptr->GetSize();

    // 能借出最后一个KV
    if (left_size > left_sibling_ptr->GetMinSize()) {
      // 叶子节点
      if (page_ptr->IsLeafPage()) {
        LOG_DEBUG("left sibling, leaf page");

        auto leaf_ptr = static_cast<LeafPage *>(page_ptr);
        auto left_leaf_ptr = static_cast<LeafPage *>(left_sibling_ptr);

        // 左兄弟的最后一个KV
        leaf_ptr->InsertKV(left_leaf_ptr->KeyAt(left_size - 1), left_leaf_ptr->ValueAt(left_size - 1), comparator_);
        left_leaf_ptr->IncreaseSize(-1);

        // 更新父节点中本page对应的key,由于向左兄弟借的，因此page_id不可能是0
        int index = parent_page_ptr->FindValue(leaf_ptr->GetPageId());
        BUSTUB_ASSERT(index >= 0, "VALUE NOT FOUND");
        parent_page_ptr->SetKeyAt(index, leaf_ptr->KeyAt(0));

        // 解除锁定
        raw_left_ptr->WUnlatch();
        buffer_pool_manager_->UnpinPage(left_leaf_ptr->GetPageId(), true);

      } else if (page_ptr->IsRootPage()) {  // 内部节点
        LOG_DEBUG("left sibling, internal page");

        auto inter_ptr = static_cast<InternalPage *>(page_ptr);
        auto left_inter_ptr = static_cast<InternalPage *>(left_sibling_ptr);

        // 修改被移动项的父节点
        page_id_t p_id = left_inter_ptr->ValueAt(left_size - 1);
        Page *raw_ptr = buffer_pool_manager_->FetchPage(p_id);
        auto ptr = reinterpret_cast<BPlusTreePage *>(raw_ptr->GetData());

        auto iter =
            std::find(pages_wlatch.begin(), pages_wlatch.end(), std::pair<Page *, BPlusTreePage *>(raw_ptr, ptr));
        bool need_latch = (iter == pages_wlatch.end());
        if (need_latch) {
          LOG_DEBUG("need latch, page id = %d", p_id);
          raw_ptr->WLatch();
        }

        ptr->SetParentPageId(inter_ptr->GetPageId());

        if (need_latch) {
          raw_ptr->WUnlatch();
        }
        buffer_pool_manager_->UnpinPage(p_id, true);

        // 更新key
        int index = parent_page_ptr->FindValue(inter_ptr->GetPageId());
        BUSTUB_ASSERT(index >= 0, "VALUE NOT FOUND");
        inter_ptr->SetKeyAt(0, parent_page_ptr->KeyAt(index));

        for (int i = inter_ptr->GetSize() - 1; i >= 0; i--) {
          inter_ptr->ItemAt(i + 1) = inter_ptr->ItemAt(i);
        }
        inter_ptr->SetKeyAt(0, left_inter_ptr->KeyAt(left_size - 1));
        inter_ptr->SetValueAt(0, left_inter_ptr->ValueAt(left_size - 1));
        inter_ptr->IncreaseSize(1);
        left_inter_ptr->IncreaseSize(-1);

        parent_page_ptr->SetKeyAt(index, inter_ptr->KeyAt(0));

        // 解除锁定
        raw_left_ptr->WUnlatch();
        buffer_pool_manager_->UnpinPage(left_inter_ptr->GetPageId(), true);

      } else {
        raw_left_ptr->WUnlatch();
        buffer_pool_manager_->UnpinPage(left_sibling_ptr->GetPageId(), false);
        BUSTUB_ASSERT(false, "INVALID PAGE TYPE");
      }
      return true;
    }
    // 不能借出
    raw_left_ptr->WUnlatch();
    buffer_pool_manager_->UnpinPage(left_sibling_id, false);
  }

  // 有右兄弟
  if (right_sibling_id != INVALID_PAGE_ID) {
    LOG_DEBUG("borrow from right sibling, page id = %d", right_sibling_id);

    Page *raw_right_ptr = buffer_pool_manager_->FetchPage(right_sibling_id);
    raw_right_ptr->WLatch();

    right_sibling_ptr = reinterpret_cast<BPlusTreePage *>(raw_right_ptr->GetData());
    int right_size = right_sibling_ptr->GetSize();

    // 能借出第一个KV
    if (right_size > right_sibling_ptr->GetMinSize()) {
      // 叶子节点
      if (page_ptr->IsLeafPage()) {
        LOG_DEBUG("right sibling, leaf page");

        auto leaf_ptr = static_cast<LeafPage *>(page_ptr);
        auto right_leaf_ptr = static_cast<LeafPage *>(right_sibling_ptr);

        // 右兄弟的一个KV
        leaf_ptr->ItemAt(leaf_ptr->GetSize()) = right_leaf_ptr->ItemAt(0);
        leaf_ptr->IncreaseSize(1);
        right_leaf_ptr->RemoveKey(right_leaf_ptr->KeyAt(0), comparator_);

        // 更新父节点中右兄弟对应的key
        int index = parent_page_ptr->FindValue(right_leaf_ptr->GetPageId());
        parent_page_ptr->SetKeyAt(index, right_leaf_ptr->KeyAt(0));

        // 解除锁定
        raw_right_ptr->WUnlatch();
        buffer_pool_manager_->UnpinPage(right_leaf_ptr->GetPageId(), true);

      } else if (page_ptr->IsRootPage()) {  // 内部节点
        LOG_DEBUG("right sibling, internal page");

        auto inter_ptr = static_cast<InternalPage *>(page_ptr);
        auto right_inter_ptr = static_cast<InternalPage *>(right_sibling_ptr);

        // 修改被移动项的父节点
        page_id_t p_id = right_inter_ptr->ValueAt(0);
        Page *raw_ptr = buffer_pool_manager_->FetchPage(p_id);
        auto ptr = reinterpret_cast<BPlusTreePage *>(raw_ptr->GetData());

        auto iter =
            std::find(pages_wlatch.begin(), pages_wlatch.end(), std::pair<Page *, BPlusTreePage *>(raw_ptr, ptr));
        bool need_latch = (iter == pages_wlatch.end());
        if (need_latch) {
          LOG_DEBUG("need latch, page id = %d", p_id);
          raw_ptr->WLatch();
        }

        ptr->SetParentPageId(inter_ptr->GetPageId());

        if (need_latch) {
          raw_ptr->WUnlatch();
        }
        buffer_pool_manager_->UnpinPage(p_id, true);

        // 更新key
        int index = parent_page_ptr->FindValue(right_inter_ptr->GetPageId());
        BUSTUB_ASSERT(index >= 0, "VALUE NOT FOUND");
        right_inter_ptr->SetKeyAt(0, parent_page_ptr->KeyAt(index));

        inter_ptr->ItemAt(inter_ptr->GetSize()) = right_inter_ptr->ItemAt(0);
        inter_ptr->IncreaseSize(1);
        right_inter_ptr->RemoveKV(0);

        parent_page_ptr->SetKeyAt(index, right_inter_ptr->KeyAt(0));

        // 解除锁定
        raw_right_ptr->WUnlatch();
        buffer_pool_manager_->UnpinPage(right_inter_ptr->GetPageId(), true);
      } else {
        raw_right_ptr->WUnlatch();
        buffer_pool_manager_->UnpinPage(right_sibling_ptr->GetPageId(), false);
        LOG_DEBUG("page type invalid.");
      }
      return true;
    }
    // 不能借出
    raw_right_ptr->WUnlatch();
    buffer_pool_manager_->UnpinPage(right_sibling_id, false);
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Merge(BPlusTreePage *page_ptr, InternalPage *parent_page_ptr, WlatchVector &pages_wlatch) {
  page_id_t left_sibling_id;
  page_id_t right_sibling_id;
  BPlusTreePage *left_sibling_ptr;
  BPlusTreePage *right_sibling_ptr;
  parent_page_ptr->GetSibling(page_ptr->GetPageId(), left_sibling_id, right_sibling_id, comparator_);

  if (left_sibling_id != INVALID_PAGE_ID) {
    LOG_DEBUG("merge with left sibling, id = %d", left_sibling_id);

    Page *raw_left_ptr = buffer_pool_manager_->FetchPage(left_sibling_id);
    raw_left_ptr->WLatch();

    left_sibling_ptr = reinterpret_cast<BPlusTreePage *>(raw_left_ptr->GetData());

    MergePage(left_sibling_ptr, page_ptr, parent_page_ptr, pages_wlatch);

    raw_left_ptr->WUnlatch();
    buffer_pool_manager_->UnpinPage(left_sibling_id, true);

    return;
  }

  if (right_sibling_id != INVALID_PAGE_ID) {
    LOG_DEBUG("merge with right sibling. %d", right_sibling_id);
    Page *raw_right_ptr = buffer_pool_manager_->FetchPage(right_sibling_id);
    raw_right_ptr->WLatch();

    right_sibling_ptr = reinterpret_cast<BPlusTreePage *>(raw_right_ptr->GetData());
    MergePage(page_ptr, right_sibling_ptr, parent_page_ptr, pages_wlatch);

    raw_right_ptr->WUnlatch();
    buffer_pool_manager_->UnpinPage(right_sibling_id, true);

    return;
  }
  BUSTUB_ASSERT(false, "Neither left nor right sibling exists");
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergePage(BPlusTreePage *left_page_ptr, BPlusTreePage *right_page_ptr,
                               InternalPage *parent_page_ptr, WlatchVector &pages_wlatch) {
  int left_size = left_page_ptr->GetSize();
  int right_size = right_page_ptr->GetSize();
  BUSTUB_ASSERT(left_size + right_size <= left_page_ptr->GetMaxSize(), "size overflow");

  // 合并叶子节点
  if (left_page_ptr->IsLeafPage()) {
    auto left_leaf_ptr = static_cast<LeafPage *>(left_page_ptr);
    auto right_leaf_ptr = static_cast<LeafPage *>(right_page_ptr);

    // 复制KV
    for (int i = left_size; i < left_size + right_size; i++) {
      left_leaf_ptr->ItemAt(i) = right_leaf_ptr->ItemAt(i - left_size);
    }
    left_leaf_ptr->IncreaseSize(right_size);

    // 修改下一页id
    left_leaf_ptr->SetNextPageId(right_leaf_ptr->GetNextPageId());

    // 删除右叶子节点在父节点中的KV
    int index = parent_page_ptr->FindValue(right_leaf_ptr->GetPageId());
    BUSTUB_ASSERT(index >= 0, "VALUE NOT FOUND");
    parent_page_ptr->RemoveKV(index);

    // 返回
    return;
  }

  // 合并内部节点
  if (left_page_ptr->IsRootPage()) {
    auto left_inter_ptr = static_cast<InternalPage *>(left_page_ptr);
    auto right_inter_ptr = static_cast<InternalPage *>(right_page_ptr);

    // 修改子结点的parent_id
    page_id_t left_inter_id = left_inter_ptr->GetPageId();
    for (int i = 0; i < right_size; i++) {
      page_id_t p_id = right_inter_ptr->ValueAt(i);
      Page *raw_ptr = buffer_pool_manager_->FetchPage(p_id);
      auto ptr = reinterpret_cast<BPlusTreePage *>(raw_ptr->GetData());

      auto iter = std::find(pages_wlatch.begin(), pages_wlatch.end(), std::pair<Page *, BPlusTreePage *>(raw_ptr, ptr));
      bool need_latch = (iter == pages_wlatch.end());
      if (need_latch) {
        raw_ptr->WLatch();
      }

      ptr->SetParentPageId(left_inter_id);

      if (need_latch) {
        raw_ptr->WUnlatch();
      }
      buffer_pool_manager_->UnpinPage(p_id, true);
    }

    // 复制KV
    int index = parent_page_ptr->FindValue(right_inter_ptr->GetPageId());
    BUSTUB_ASSERT(index >= 0, "VALUE NOT FOUND");
    right_inter_ptr->KeyAt(0) = parent_page_ptr->KeyAt(index);

    for (int i = left_size; i < left_size + right_size; i++) {
      left_inter_ptr->ItemAt(i) = right_inter_ptr->ItemAt(i - left_size);
    }
    left_inter_ptr->IncreaseSize(right_size);

    // 删除右内部节点在父节点中KV
    parent_page_ptr->RemoveKV(index);

    // 返回
    return;
  }

  BUSTUB_ASSERT(false, "page type invalid.");
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    // LOG_DEBUG("Tree is empty.");
    return INDEXITERATOR_TYPE();
  }

  page_id_t page_id = root_page_id_;
  auto page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
  while (true) {
    if (page->IsRootPage()) {
      auto r_page = reinterpret_cast<InternalPage *>(page);
      page_id_t temp = r_page->ValueAt(0);

      buffer_pool_manager_->UnpinPage(page_id, false);
      page_id = temp;

      page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());

    } else if (page->IsLeafPage()) {
      buffer_pool_manager_->UnpinPage(page_id, false);
      break;

    } else {
      BUSTUB_ASSERT(false, "INVALID_INDEX_PAGE");
      buffer_pool_manager_->UnpinPage(page_id, false);
      return INDEXITERATOR_TYPE();
    }
  }

  return INDEXITERATOR_TYPE(buffer_pool_manager_, page_id, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  // LOG_DEBUG("tree.begin");
  page_id_t page_id;
  page_id_t next_page_id;
  Page *raw_page_ptr = nullptr;
  LeafPage *page_ptr = nullptr;
  int index;
  bool get_page = GetLeafPageOptimistic(key, &raw_page_ptr, &page_ptr, nullptr, false);
  if (!get_page) {
    return INDEXITERATOR_TYPE();
  }
  page_id = page_ptr->GetPageId();

  index = page_ptr->FindKey(key, comparator_);
  next_page_id = page_ptr->GetNextPageId();

  if (index < page_ptr->GetSize()) {
    // LOG_DEBUG("tree.begin: this page, page_id = %d, index = %d", page_id, index);
    buffer_pool_manager_->UnpinPage(page_id, false);
    return INDEXITERATOR_TYPE(buffer_pool_manager_, page_id, index);
  }

  if (page_ptr->GetNextPageId() == INVALID_PAGE_ID) {
    // LOG_DEBUG("tree.begin: return end");
    buffer_pool_manager_->UnpinPage(page_id, false);
    return INDEXITERATOR_TYPE(buffer_pool_manager_, page_id, index);
  }

  // LOG_DEBUG("tree.begin: next page, index = 0");
  buffer_pool_manager_->UnpinPage(page_id, false);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, next_page_id, 0);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  LOG_DEBUG("tree.end");
  if (IsEmpty()) {
    LOG_DEBUG("Tree is empty.");
    return INDEXITERATOR_TYPE();
  }

  page_id_t page_id = root_page_id_;
  int index_end = 0;
  auto page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
  while (true) {
    if (page->IsRootPage()) {
      auto r_page = reinterpret_cast<InternalPage *>(page);
      index_end = r_page->GetSize() - 1;
      page_id_t temp = r_page->ValueAt(index_end);

      buffer_pool_manager_->UnpinPage(page_id, false);
      page_id = temp;

      page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());

    } else if (page->IsLeafPage()) {
      index_end = page->GetSize() - 1;
      buffer_pool_manager_->UnpinPage(page_id, false);
      break;

    } else {
      BUSTUB_ASSERT(false, "INVALID_INDEX_PAGE");
      buffer_pool_manager_->UnpinPage(page_id, false);
      return INDEXITERATOR_TYPE();
    }
  }
  // 返回最右叶子节点的超尾
  // LOG_DEBUG("tree.end page_id = %d, index = %d", page_id, index_end + 1);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, page_id, index_end + 1);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      auto GetLeafPage(const KeyType &key, page_id_t &page_id, LeafPage **page_pptr)->bool;

      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
