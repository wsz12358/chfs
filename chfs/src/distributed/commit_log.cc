#include <algorithm>

#include "common/bitmap.h"
#include "distributed/commit_log.h"
#include "distributed/metadata_server.h"
#include "filesystem/directory_op.h"
#include "metadata/inode.h"
#include <chrono>

namespace chfs {
/**
 * `CommitLog` part
 */
// {Your code here}
CommitLog::CommitLog(std::shared_ptr<BlockManager> bm,
                     bool is_checkpoint_enabled)
    : is_checkpoint_enabled_(is_checkpoint_enabled), bm_(bm) {
}

CommitLog::~CommitLog() {}

// {Your code here}
auto CommitLog::get_log_entry_num() -> usize {
  // TODO: Implement this function.
  return entry_num;
}

// {Your code here}
auto CommitLog::append_log(txn_id_t txn_id,
                           std::vector<std::shared_ptr<BlockOperation>> ops)
    -> void {
  // TODO: Implement this function.
  if (max_txn <= txn_id) max_txn = txn_id;
  this->entry_num += ops.size();
  if (this->logs.find(txn_id) != this->logs.end())
  {
    auto log = (this->logs)[txn_id];
    for (auto it = ops.begin(); it != ops.end(); it++)
    {
        log.push_back((*it));
    }
    (this->logs)[txn_id] = log;
  }
  else
  {
    this->logs.insert(std::pair<txn_id_t, std::vector<std::shared_ptr<BlockOperation>>>(txn_id, ops));
  }
  if (this->entry_num > 100)
  {
      for(int i = 1; i <= max_txn; i++)
  {
    if (logs.find(i) != logs.end())
    {
      commit_log(i);
    }
  }
  }
}

// {Your code here}
auto CommitLog::commit_log(txn_id_t txn_id) -> void {
  // TODO: Implement this function.
  auto log = this->logs[txn_id];
      for (auto it = log.begin(); it != log.end(); it++)
      {
          u8 * ptr = this->bm_->block_data;
          ptr += (*it)->block_id_ * this->bm_->block_sz;
          auto data = (*it)->new_block_state_.data();
        for (int j = 0; j < this->bm_->block_sz / sizeof(u8); j++)
        {
          *(ptr + j) = * (data + j);
        }
      }
  entry_num -= log.size();
  logs.erase(txn_id);
}

// {Your code here}
auto CommitLog::checkpoint() -> void {
  // TODO: Implement this function.
  UNIMPLEMENTED();
}

// {Your code here}
auto CommitLog::recover() -> void {
  // TODO: Implement this function.
  for(int i = 1; i <= max_txn; i++)
  {
    if (logs.find(i) != logs.end())
    {
      auto log = this->logs[i];
      for (auto it = log.begin(); it != log.end(); it++)
      {
          u8 * ptr = this->bm_->block_data;
          ptr += (*it)->block_id_ * this->bm_->block_sz;
          auto data = (*it)->new_block_state_.data();
        for (int j = 0; j < this->bm_->block_sz / sizeof(u8); j++)
        {
          *(ptr + j) = * (data + j);
        }
      }
    }
  }
}
}; // namespace chfs