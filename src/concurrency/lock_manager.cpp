//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::AssignLock(Transaction *txn, LockMode lock_mode, const std::shared_ptr<LockRequestQueue> lock_queue, ResourceType type) -> bool {
  LOG_DEBUG("AssignLock called. txn = %d, lock_mode = %d", txn->GetTransactionId(), int(lock_mode));
  
  std::unordered_map<LockMode, bool> lock_allowed = {};
  int count;

  if (type == ResourceType::TBALE) {
    lock_allowed = std::unordered_map<LockMode, bool>{{LockMode::INTENTION_SHARED, true}, {LockMode::SHARED, true}, {LockMode::INTENTION_EXCLUSIVE, true}, 
    {LockMode::SHARED_INTENTION_EXCLUSIVE, true}, {LockMode::EXCLUSIVE, true}};
    count = 5;
  } else {
    lock_allowed = std::unordered_map<LockMode, bool>{{LockMode::SHARED, true}, {LockMode::EXCLUSIVE, true}};
    count = 2;
  }
  
  // 判断本事务是否是upgrade, 若是，赋锁前需要unlock旧锁
  bool is_upgrade = txn->GetTransactionId() == lock_queue->upgrading_;
  std::list<LockRequest *>::iterator old_request_iter;
  
  // 遍历现有的已经授予的锁，得到能够授予的锁类型
  for (auto iter = lock_queue->request_queue_.begin(); iter != lock_queue->request_queue_.end(); iter++){
    if (count == 0) {
      break;
    }
    
    if (is_upgrade && (*iter)->granted_ && (*iter)->txn_id_ == lock_queue->upgrading_) {
      old_request_iter = iter;
    }

    if ((*iter)->granted_ && lock_queue->upgrading_ != (*iter)->txn_id_) {
      for (auto lm : incompatible_mode_[(*iter)->lock_mode_]) {   // 遍历当前锁不兼容的锁类型
        if (lock_allowed[lm]) {
          count--;
          lock_allowed[lm] = false;
        }
      }
    }
  }

  // 所有锁类型均不能授予
  if (count == 0) {
    return false;
  }
  
  for (auto& request : lock_queue->request_queue_) {
    if (!request->granted_) {
      if (lock_allowed[request->lock_mode_]) {  //能够被授予锁
        // 如果自身的请求能够被授予锁，实际授予锁
        if (request->txn_id_ == txn->GetTransactionId()) {
          request->granted_ = true;

          InsertLockSet(txn, type, request->lock_mode_, request->oid_, request->rid_);
        
          // upgrade
          if (is_upgrade) {
            LockRequest* old_request = *old_request_iter;
            EraseLockSet(txn, type, old_request->lock_mode_, old_request->oid_, old_request->rid_);
            lock_queue->request_queue_.erase(old_request_iter);
            delete old_request;

            // 清除upgrade标志
            lock_queue->upgrading_ = INVALID_TXN_ID;
          }

          return true;
        }

        // 假设授予，更新允许授予的锁类型
        for (auto lm : incompatible_mode_[request->lock_mode_]) {
          lock_allowed[lm] = false;
        }        
      } else {                        // 找到第一个不能被授予锁的请求，终止
        break;
      }
    }
  }
  return false;
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool { 
    LOG_DEBUG("LockTable called. txn = %d, lock_mode = %d", txn->GetTransactionId(), int(lock_mode));
    // 检查当前隔离等级、事务状态、资源类型下，请求获取的锁类型是否合法
    CheckLockModeLegal(txn, lock_mode, ResourceType::TBALE);

    // 检查本事务当前是否持有本表格的锁
    // -1：已经持有申请的锁，直接返回；0：未持有锁，正常请求锁；1：持有较低级别的锁，申请升级锁；
    // 若已持有的锁和申请的锁不符合升级条件，抛出异常
    RID dummy_rid;
    int res = CheckLockUpgradeLegal(txn, ResourceType::TBALE, lock_mode, oid, dummy_rid);
    if (res == -1) {
      return true;
    }

    // 获取Lock队列
    table_lock_map_latch_.lock();
    if (table_lock_map_.count(oid) == 0) {
      table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
    }
    std::shared_ptr<LockRequestQueue> lock_queue = table_lock_map_[oid];
    table_lock_map_latch_.unlock();
    
    // 获取队列latch
    std::unique_lock<std::mutex> lck(lock_queue->latch_);

    // 新建lock请求
    LockRequest* req = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
    if (res == 1) {   // upgrade
      if (lock_queue->upgrading_ == INVALID_TXN_ID) {
        lock_queue->upgrading_ = txn->GetTransactionId();
      } else {
        // 存在尚未处理的upgrade请求
        delete req;
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      
      // upgrade请求具有最高优先级，放在请求队列头部
      lock_queue->request_queue_.push_front(req);
    } else {
      // 正常请求，FIFO, 放在请求队列尾部
      lock_queue->request_queue_.push_back(req);
    }

    // 检查本事务能否被授予锁
    while (!AssignLock(txn, lock_mode, lock_queue, ResourceType::TBALE)) {
      LOG_DEBUG("AssignLock fail, waiting. txn = %d", txn->GetTransactionId());
      lock_queue->PrintQueue();
      lock_queue->cv_.wait(lck);
      // 事务可能在waiting过程中，由于死锁检测被设置为aborted，需要取消请求
      if (txn->GetState() == TransactionState::ABORTED) {
        for (auto iter = lock_queue->request_queue_.begin(); iter != lock_queue->request_queue_.end(); iter++) {
          LockRequest* request = *iter;
          if (request->txn_id_ == txn->GetTransactionId() && !request->granted_ && request->lock_mode_ == lock_mode) {
            lock_queue->request_queue_.erase(iter);
            delete request;
            break;
          }
        }
        return false;
      }
    }

    LOG_DEBUG("AssignLock success, return. txn = %d", txn->GetTransactionId());
    lock_queue->PrintQueue();
    return true;
 }

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // 检查当前隔离等级、事务状态、资源类型下，请求获取的锁类型是否合法
  CheckLockModeLegal(txn, lock_mode, ResourceType::TBALE);
  
  // 检查本事务当前是否持有本行的锁
  // -1：已经持有申请的锁，直接返回；0：未持有锁，正常请求锁；1：持有较低级别的锁，申请升级锁；
  // 若已持有的锁和申请的锁不符合升级条件，抛出异常
  int res = CheckLockUpgradeLegal(txn, ResourceType::ROW, lock_mode, oid, rid);
  if (res == -1) {
    return true;
  }
  
  row_lock_map_latch_.lock();
  if (row_lock_map_.count(rid) == 0) {
      row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  std::shared_ptr<LockRequestQueue> lock_queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();

  std::unique_lock<std::mutex> lck(lock_queue->latch_);
  LockRequest* req = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);
  if (res == 1) {
    if (lock_queue->upgrading_ == INVALID_TXN_ID) {
      lock_queue->upgrading_ = txn->GetTransactionId();
    } else {
      // 存在尚未处理的upgrade请求
      txn->SetState(TransactionState::ABORTED);
      delete req;
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }

    // upgrade请求具有最高优先级，放在请求队列头部
    lock_queue->request_queue_.push_front(req);
  } else {
    // 正常请求，FIFO, 放在请求队列尾部
    lock_queue->request_queue_.push_back(req);
  }

  // 检查本事务能否被授予锁
  while (!AssignLock(txn, lock_mode, lock_queue, ResourceType::ROW)) {
    lock_queue->cv_.wait(lck);
    // 事务可能在waiting过程中，由于死锁检测被设置为aborted
    if (txn->GetState() == TransactionState::ABORTED) {
      for (auto iter = lock_queue->request_queue_.begin(); iter != lock_queue->request_queue_.end(); iter++) {
        LockRequest* request = *iter;
        if (request->txn_id_ == txn->GetTransactionId() && !request->granted_ && request->lock_mode_ == lock_mode) {
          lock_queue->request_queue_.erase(iter);
          delete request;
          break;
        }
      }
      return false;
    }
  }
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool { 
  LOG_DEBUG("UnlockTable called. txn = %d", txn->GetTransactionId());
  // 判断持有目标锁
  RID dummy_rid;
  LockMode lock_mode = GetUnlockMode(txn, ResourceType::TBALE, oid, dummy_rid);
  
  // 判断该表下不存在行锁
  if (!(*txn->GetSharedRowLockSet())[oid].empty() || !(*txn->GetExclusiveRowLockSet())[oid].empty()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  
  // 获取lock队列
  table_lock_map_latch_.lock();
  std::shared_ptr<LockRequestQueue> lock_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();

  // 获取队列latch
  std::unique_lock<std::mutex> lck(lock_queue->latch_);
  
  for (auto iter = lock_queue->request_queue_.begin(); iter != lock_queue->request_queue_.end(); iter++) {
    // 遍历请求队列，找到该事务对应的锁请求并清除
    if ((*iter)->txn_id_ == txn->GetTransactionId()) {
      LockRequest* req = *iter;
      if (!(*iter)->granted_) {
        LOG_DEBUG("try to unlock a unlocked table!, txn = %d", txn->GetTransactionId());
      }

      if ((*iter)->lock_mode_ != lock_mode) {
        LOG_DEBUG("unlock mode dismatch, txn = %d, lock mode = %d, but try to unlock = %d", txn->GetTransactionId(), int(lock_mode), int((*iter)->lock_mode_));
      }
      
      BUSTUB_ASSERT((*iter)->granted_ == true, "table unlocked");
      BUSTUB_ASSERT((*iter)->lock_mode_ == lock_mode, "lock mode is not match");
      lock_queue->request_queue_.erase(iter);   // 从请求队列中清除
      delete req;
      
      RID dummy_rid;
      EraseLockSet(txn, ResourceType::TBALE, lock_mode, oid, dummy_rid);  // 从事务中清除

      break;
    }
  }

  // 修改txn状态
  TxnStates2Shrinking(txn, lock_mode);

  // 唤醒等待该表资源的其他线程，使其检查自身能否获取锁
  lock_queue->cv_.notify_all();
  
  return true;
 }

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool { 
  // 判断持有目标锁
  LockMode lock_mode = GetUnlockMode(txn, ResourceType::ROW, oid, rid);

  // 获取lock队列
  row_lock_map_latch_.lock();
  std::shared_ptr<LockRequestQueue> lock_queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();

  // 获取队列latch
  std::unique_lock<std::mutex> lck(lock_queue->latch_);

  for (auto iter = lock_queue->request_queue_.begin(); iter != lock_queue->request_queue_.end(); iter++) {
    // 遍历请求队列，找到该事务的锁请求并清除
    if ((*iter)->txn_id_ == txn->GetTransactionId()) {
      LockRequest* req = *iter;
      BUSTUB_ASSERT(req->granted_ == true, "row unlocked");
      BUSTUB_ASSERT(req->lock_mode_ == lock_mode, "lock mode is not match");
      iter = lock_queue->request_queue_.erase(iter);
      delete req;

      EraseLockSet(txn, ResourceType::ROW, lock_mode, oid, rid);  
      
      // 修改txn状态
      TxnStates2Shrinking(txn, lock_mode);

      // 唤醒等待该表资源的其他线程，使其检查自身能否获取锁
      lock_queue->cv_.notify_all();

      break;
    }
  }
  return true;
 }

/**
*    REPEATABLE_READ:
*        The transaction is required to take all locks.
*        All locks are allowed in the GROWING state
*        No locks are allowed in the SHRINKING state
*
*    READ_COMMITTED:
*        The transaction is required to take all locks.
*        All locks are allowed in the GROWING state
*        Only IS, S locks are allowed in the SHRINKING state
*
*    READ_UNCOMMITTED:
*        The transaction is required to take only IX, X locks.
*        X, IX locks are allowed in the GROWING state.
*        S, IS, SIX locks are never allowed
**/
void LockManager::CheckLockModeLegal(Transaction *txn, LockMode lock_mode, ResourceType type) {
  auto isolation_level = txn->GetIsolationLevel();
  auto txn_state = txn->GetState();

  if (type == ResourceType::ROW && !(lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  
  if (isolation_level == IsolationLevel::REPEATABLE_READ) {
    if (txn_state == TransactionState::SHRINKING) {   // REPEATABLE_READ下，不允许在shrinking阶段获取锁
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  } else if (isolation_level == IsolationLevel::READ_COMMITTED) {
    if (txn_state == TransactionState::SHRINKING) {   // READ_COMMITTED下，不允许在shrinking阶段获取写锁
      if (lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    }
  } else if (isolation_level == IsolationLevel::READ_UNCOMMITTED) {
    if (txn_state == TransactionState::GROWING) {   // READ_UNCOMMITTED下，只存在写锁，并且只允许在growing阶段获取
      if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
    } else {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
}

auto LockManager::CheckLockUpgradeLegal(Transaction *txn, ResourceType type, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> int {
  std::optional<LockMode> old_lock_mode;
  if (type == ResourceType::TBALE) {
    if (txn->IsTableSharedLocked(oid)) {
      old_lock_mode = LockMode::SHARED;
    } else if (txn->IsTableExclusiveLocked(oid)) {
      old_lock_mode = LockMode::EXCLUSIVE;
    } else if (txn->IsTableIntentionSharedLocked(oid)) {
      old_lock_mode = LockMode::INTENTION_SHARED;
    } else if (txn->IsTableIntentionExclusiveLocked(oid)) {
      old_lock_mode = LockMode::INTENTION_EXCLUSIVE;
    } else if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      old_lock_mode = LockMode::SHARED_INTENTION_EXCLUSIVE;
    }
  } else if (type == ResourceType::ROW) {
    if (txn->IsRowSharedLocked(oid, rid)) {
      old_lock_mode = LockMode::SHARED;
    } else if (txn->IsRowExclusiveLocked(oid, rid)) {
      old_lock_mode = LockMode::EXCLUSIVE;
    }
  }

  // 没有持有锁，不需要upgrade
  if (!old_lock_mode.has_value()) {
    return 0;
  }

  // 已持有目标锁
  if (old_lock_mode.value() == lock_mode) {
    return -1;
  }

  bool upgrade = false;
  switch (old_lock_mode.value())
  {
  case LockMode::SHARED:
    upgrade = (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE);
    break;
  
  case LockMode::EXCLUSIVE:
    break;

  case LockMode::INTENTION_SHARED:
    upgrade = (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE || 
              lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE);
    break;
  
  case LockMode::INTENTION_EXCLUSIVE:
    upgrade = (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE);
    break;

  case LockMode::SHARED_INTENTION_EXCLUSIVE:
    upgrade = (lock_mode == LockMode::EXCLUSIVE);
    break;
  }

  if (upgrade) {
    return 1;
  } else {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
  }
}

auto LockManager::GetUnlockMode(Transaction *txn, ResourceType type, const table_oid_t &oid, const RID &rid) -> LockMode {
  LockMode lock_mode = LockMode::SHARED;

  if (type == ResourceType::TBALE) {
    if (txn->IsTableSharedLocked(oid)) {
      lock_mode = LockMode::SHARED;
    } else if (txn->IsTableExclusiveLocked(oid)) {
      lock_mode = LockMode::EXCLUSIVE;
    } else if (txn->IsTableIntentionExclusiveLocked(oid)) {
      lock_mode = LockMode::INTENTION_EXCLUSIVE;
    } else if (txn->IsTableIntentionSharedLocked(oid)) {
      lock_mode = LockMode::INTENTION_SHARED;
    } else if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      lock_mode = LockMode::SHARED_INTENTION_EXCLUSIVE;
    } else {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    }
  } else if (type == ResourceType::ROW) {
    if (txn->IsRowSharedLocked(oid, rid)) {
      lock_mode = LockMode::SHARED;
    } else if (txn->IsRowExclusiveLocked(oid, rid)) {
      lock_mode = LockMode::EXCLUSIVE;
    } else {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    }
  } else {
    LOG_DEBUG("invalid resource type");
  }

  return lock_mode;
}

void LockManager::TxnStates2Shrinking(Transaction *txn, LockMode lock_mode) {
  if (txn->GetState() != TransactionState::GROWING) {
    return;
  }

  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
        txn->SetState(TransactionState::SHRINKING);
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && 
      lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
      lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
  } else {
    LOG_DEBUG("invalid isolation level");
  }
}

void LockManager::EraseLockSet(Transaction *txn, ResourceType type, LockMode lock_mode, const table_oid_t& oid, const RID& rid) {
  if (type == ResourceType::TBALE) {
    switch (lock_mode)
    {
      case LockMode::INTENTION_SHARED:
        txn->GetIntentionSharedTableLockSet()->erase(oid);   // IS
        break;
      case LockMode::SHARED:
        txn->GetSharedTableLockSet()->erase(oid);    // S
        break;
      case LockMode::INTENTION_EXCLUSIVE:
        txn->GetIntentionExclusiveTableLockSet()->erase(oid);  // IX
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);  // SIX
        break;
      case LockMode::EXCLUSIVE:
        txn->GetExclusiveTableLockSet()->erase(oid);
        break;
      default:
        LOG_DEBUG("invalid table lock mode");
        break;
    }
  } else if (type == ResourceType::ROW){
      switch (lock_mode) {
        case LockMode::SHARED:
          txn->GetSharedRowLockSet()->at(oid).erase(rid);
          break;
        case LockMode::EXCLUSIVE:
          txn->GetExclusiveRowLockSet()->at(oid).erase(rid);
          break;
        default:
          LOG_DEBUG("invalid row lock mode");
          break;
      }
  } else {
    LOG_DEBUG("invalid resource type");
  }
}

void LockManager::InsertLockSet(Transaction *txn, ResourceType type, LockMode lock_mode, const table_oid_t& oid, const RID& rid) {
  if (type == ResourceType::TBALE) {
    switch (lock_mode)
    {
      case LockMode::INTENTION_SHARED:
        txn->GetIntentionSharedTableLockSet()->insert(oid);   // IS
        break;
      case LockMode::SHARED:
        txn->GetSharedTableLockSet()->insert(oid);    // S
        break;
      case LockMode::INTENTION_EXCLUSIVE:
        txn->GetIntentionExclusiveTableLockSet()->insert(oid);  // IX
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);  // SIX
        break;
      case LockMode::EXCLUSIVE:
        txn->GetExclusiveTableLockSet()->insert(oid);
        break;
      default:
        LOG_DEBUG("invalid table lock mode");
        break;
    }
  } else if (type == ResourceType::ROW) {
      switch (lock_mode) {
        case LockMode::SHARED:
          if (txn->GetSharedRowLockSet()->count(oid) == 0) {
            txn->GetSharedRowLockSet()->insert({oid, std::unordered_set<bustub::RID>()});
          }
          txn->GetSharedRowLockSet()->at(oid).insert(rid);
          break;
        case LockMode::EXCLUSIVE:
          if (txn->GetExclusiveRowLockSet()->count(oid) == 0) {
            txn->GetExclusiveRowLockSet()->insert({oid, std::unordered_set<bustub::RID>()});
          }
          txn->GetExclusiveRowLockSet()->at(oid).insert(rid);
          break;
        default:
          LOG_DEBUG("invalid row lock mode");
          break;
      }
  } else {
    LOG_DEBUG("invalid resource type");
  }
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  // t1 is waiting for t2
  std::lock_guard<std::mutex> lck(waits_for_latch_);
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  std::lock_guard<std::mutex> lck(waits_for_latch_);
  std::vector<txn_id_t>& txn_vec = waits_for_[t1];
  txn_vec.erase(std::remove(txn_vec.begin(), txn_vec.end(), t2), txn_vec.end());
}

auto LockManager::DFS(txn_id_t curr, std::set<txn_id_t>& not_visited, std::unordered_map<txn_id_t, txn_id_t>& path, txn_id_t *txn_id) -> bool {
  not_visited.erase(curr);

  // 如果该节点在path中，表明成环
  if (path.find(curr) != path.end()) {
    txn_id_t max_txn = curr;
    txn_id_t next = path[curr];
    while (next != curr) {
      max_txn = std::max(max_txn, next);
      next = path[next];
    }
    *txn_id = max_txn;
    return true;
  }

  for (auto neighbor : waits_for_[curr]) {  // neibor从小到大排列
    path[curr] = neighbor;
    if (DFS(neighbor, not_visited, path, txn_id)) {
      return true;
    }
  }
  path.erase(curr);

  return false;
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { 
  std::set<txn_id_t> not_visited;
  for (auto& kv : waits_for_) {
    not_visited.insert(kv.first);
  }

  while (!not_visited.empty()) {
    // 从未被访问的最小节点开始DFS遍历
    // 若DFS返回false，表明访问到的所有节点都不是环的一部分，就需要再以该节点为起点进行DFS，减少不必要的遍历
    // 若DFS返回true，表明找到环，直接返回
    std::unordered_map<txn_id_t, txn_id_t> path;
    if (DFS(*not_visited.begin(), not_visited, path, txn_id)) {
      return true;
    }
  }
  
  return false;
}

void LockManager::BuildWaitsForMap() {
  std::unique_lock<std::mutex> table_lck(table_lock_map_latch_);
  for (auto& kv : table_lock_map_) {
    std::lock_guard<std::mutex> queue_lck(kv.second->latch_);
    std::list<LockRequest *>& request_queue = kv.second->request_queue_;
    for (auto request1 : request_queue) {
      for (auto request2 : request_queue) {
        // request1 等待 request2
        if (request1->txn_id_ != request2->txn_id_ && !request1->granted_ && request2->granted_) {
          AddEdge(request1->txn_id_, request2->txn_id_);
        }
      }
    }
  }
  table_lck.unlock();

  std::unique_lock<std::mutex> row_lck(row_lock_map_latch_);
  for (auto& kv : row_lock_map_) {
    std::lock_guard<std::mutex> queue_lck(kv.second->latch_);
    std::list<LockRequest *>& request_queue = kv.second->request_queue_;
    for (auto request1 : request_queue) {
      for (auto request2 : request_queue) {
        // request1 等待 request2
        if (request1 != request2 && !request1->granted_ && request2->granted_) {
          AddEdge(request1->txn_id_, request2->txn_id_);
        }
      }
    }
  }
  row_lck.unlock();

  // 对waits_for中各节点的后继节点进行排序，保证有序的遍历
  for (auto& kv : waits_for_) {
    std::sort(kv.second.begin(), kv.second.end());
  }
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::lock_guard<std::mutex> lck(waits_for_latch_);
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (auto& kv : waits_for_) {
    for (auto& t2 : kv.second) {
      edges.emplace_back(kv.first, t2);
    }
  }
  return edges;
}

void LockManager::AbortTxn(txn_id_t txn_id) {
  Transaction* txn = TransactionManager::GetTransaction(txn_id);
  txn->SetState(TransactionState::ABORTED);

  // 释放该Txn持有的所有锁，先行锁再表锁
  auto row_lock_set = txn->GetExclusiveRowLockSet();
  std::unordered_map<table_oid_t, std::unordered_set<RID>> row_set = *row_lock_set;
  for (auto& kv : row_set) {
    for (auto rid : kv.second) {
      UnlockRow(txn, kv.first, rid);
    }
  }

  row_lock_set = txn->GetSharedRowLockSet();
  row_set = *row_lock_set;
  for (auto& kv : row_set) {
    for (auto rid : kv.second) {
      UnlockRow(txn, kv.first, rid);
    }
  }

  auto table_lock_set = txn->GetSharedTableLockSet();
  std::unordered_set<table_oid_t> table_set = *table_lock_set;
  for (auto oid : table_set) {
    UnlockTable(txn, oid);
  }

  table_lock_set = txn->GetExclusiveTableLockSet();
  table_set = *table_lock_set;
  for (auto oid : table_set) {
    UnlockTable(txn, oid);
  }

  table_lock_set = txn->GetIntentionSharedTableLockSet();
  table_set = *table_lock_set;
  for (auto oid : table_set) {
    UnlockTable(txn, oid);
  }

  table_lock_set = txn->GetIntentionExclusiveTableLockSet();
  table_set = *table_lock_set;
  for (auto oid : table_set) {
    UnlockTable(txn, oid);
  }

  table_lock_set = txn->GetSharedIntentionExclusiveTableLockSet();
  table_set = *table_lock_set;
  for (auto oid : table_set) {
    UnlockTable(txn, oid);
  }

  // 修改wait_for, 清除txn_id的全部依赖关系，使环断开
  std::lock_guard<std::mutex> lck(waits_for_latch_);
  waits_for_.erase(txn_id);
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    try {
      BuildWaitsForMap();
      txn_id_t txn_abort;
      while (HasCycle(&txn_abort)) {
        LOG_DEBUG("has cycle, abort txn = %d", txn_abort);
        AbortTxn(txn_abort);
      }
    } catch (TransactionAbortException& exp) {
          LOG_DEBUG("%s", exp.GetInfo().c_str());
    }
    }
  }
}

}  // namespace bustub
