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
void LockManager::CheckLockModeLegal(Transaction *txn, LockMode lock_mode) {
  auto isolation_level = txn->GetIsolationLevel();
  auto txn_state = txn->GetState();

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

/**
 *    While upgrading, only the following transitions should be allowed:
 *        IS -> [S, X, IX, SIX]
 *        S -> [X, SIX]
 *        IX -> [X, SIX]
 *        SIX -> [X]
 *    Any other upgrade is considered incompatible, and such an attempt should set the TransactionState as ABORTED
 *    and throw a TransactionAbortException (INCOMPATIBLE_UPGRADE)
 * 
 *    Furthermore, only one transaction should be allowed to upgrade its lock on a given resource.
 *    Multiple concurrent lock upgrades on the same resource should set the TransactionState as
 *    ABORTED and throw a TransactionAbortException (UPGRADE_CONFLICT).
 * 
 *    return： 
 *    -1：txn has already held the required lock
 *    0 : txn has no lock held
 *    1 : txn need to upgrade the held lock
*/
auto LockManager::CheckLockUpgradeLegal(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> int { 
  int res = 0;

  if (txn->IsTableSharedLocked(oid)) {
    if (lock_mode == LockMode::SHARED) {
      res = -1;
    } else if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      res = 1;
    } else {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
  }

  if (txn->IsTableIntentionSharedLocked(oid)) {
    if (lock_mode == LockMode::INTENTION_SHARED) {
      res = -1;
    } else if (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE || 
              lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      res = 1;
    } else {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
  }

  if (txn->IsTableIntentionExclusiveLocked(oid)) {
    if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
      res = -1;
    } else if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      res = 1;
    } else {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
  }

  if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      res = -1;
    } else if (lock_mode == LockMode::EXCLUSIVE) {
      res = 1;
    } else {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
  }

  if (txn->IsTableExclusiveLocked(oid)) {
    if (lock_mode == LockMode::EXCLUSIVE) {
      res = -1;
    } else {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
  }

  return res;
}

auto LockManager::AssignTableLock(Transaction *txn, LockMode lock_mode, const std::shared_ptr<LockRequestQueue> lock_queue) -> bool {
  std::unordered_map<LockMode, bool> lock_allowed;
  lock_allowed[LockMode::INTENTION_SHARED] = true;
  lock_allowed[LockMode::SHARED] = true;
  lock_allowed[LockMode::INTENTION_EXCLUSIVE] = true;
  lock_allowed[LockMode::SHARED_INTENTION_EXCLUSIVE] = true;
  lock_allowed[LockMode::EXCLUSIVE] = true;

  // 遍历现有的已经授予的锁，得到能够授予的锁类型
  int count = 5;
  for (auto& request : lock_queue->request_queue_) {
    if (request->granted_ && count > 0) {
      for (auto lm : incompatible_mode_[request->lock_mode_]) {   // 当前锁不兼容的锁类型
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

          switch (request->lock_mode_)
          {
          case LockMode::INTENTION_SHARED:
            txn->GetIntentionSharedTableLockSet()->insert(request->oid_);   // IS
            break;
          case LockMode::SHARED:
            txn->GetSharedTableLockSet()->insert(request->oid_);    // S
            break;
          case LockMode::INTENTION_EXCLUSIVE:
            txn->GetIntentionExclusiveTableLockSet()->insert(request->oid_);  // IX
            break;
          case LockMode::SHARED_INTENTION_EXCLUSIVE:
            txn->GetSharedIntentionExclusiveTableLockSet()->insert(request->oid_);  // SIX
            break;
          case LockMode::EXCLUSIVE:
            txn->GetExclusiveTableLockSet()->insert(request->oid_);
            break;
          default:
            LOG_DEBUG("invalid lock mode");
            break;
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
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool { 
    CheckLockModeLegal(txn, lock_mode);

    int res = CheckLockUpgradeLegal(txn, lock_mode, oid);
    if (res == -1) {
      return true;
    }

    table_lock_map_latch_.lock();
    std::shared_ptr<LockRequestQueue> lock_queue = table_lock_map_[oid];
    table_lock_map_latch_.unlock();
    
    std::unique_lock<std::mutex> lck(lock_queue->latch_);

    if (res == 1) {
      if (lock_queue->upgrading_ == INVALID_TXN_ID) {
        lock_queue->upgrading_ = txn->GetTransactionId();
      } else {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }

      lock_queue->request_queue_.emplace_front(txn->GetTransactionId(), lock_mode, oid);
    } else {
      lock_queue->request_queue_.emplace_back(txn->GetTransactionId(), lock_mode, oid);
    }

    // 检查Txn能否被授予锁，按照FIFO顺序查看等待队列
    while (!AssignTableLock(txn, lock_mode, lock_queue)) {
      lock_queue->cv_.wait(lck);
    }
    return true;
 }

auto LockManager::GetUnlockMode(Transaction *txn, const table_oid_t &oid) -> LockMode {
  LockMode lock_mode;
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
  return lock_mode;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool { 
  // 判断持有目标锁
  LockMode lock_mode = GetUnlockMode(txn, oid);
  
  // 判断行锁
  if (!(*txn->GetSharedRowLockSet())[oid].empty() || !(*txn->GetExclusiveRowLockSet())[oid].empty()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  
  table_lock_map_latch_.lock();
  std::shared_ptr<LockRequestQueue> lock_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();

  std::unique_lock<std::mutex> lck(lock_queue->latch_);
  
  for (auto iter = lock_queue->request_queue_.begin(); iter != lock_queue->request_queue_.end(); iter++) {
    if ((*iter)->txn_id_ == txn->GetTransactionId()) {
      if ((*iter)->lock_mode_ != lock_mode) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
      }
      lock_queue->request_queue_.erase(iter);
      break;
    }
  }

  lock_queue->cv_.notify_all();
  
  // 修改txn状态
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && 
      (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE)) {
        txn->SetState(TransactionState::SHRINKING);
      }
  else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && 
      lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
  else if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
      lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
  else {
    LOG_DEBUG("invalid isolation level");
  }

  return true;
 }

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool { return true; }

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
