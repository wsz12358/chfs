#include "distributed/metadata_server.h"
#include "common/util.h"
#include "filesystem/directory_op.h"
#include <fstream>
#include <ctime>
#include <cstdlib>
#include <mutex>
std::mutex lock;



namespace chfs {

inline auto MetadataServer::bind_handlers() {
  server_->bind("mknode",
                [this](u8 type, inode_id_t parent, std::string const &name) {
                  return this->mknode(type, parent, name);
                });
  server_->bind("unlink", [this](inode_id_t parent, std::string const &name) {
    return this->unlink(parent, name);
  });
  server_->bind("lookup", [this](inode_id_t parent, std::string const &name) {
    return this->lookup(parent, name);
  });
  server_->bind("get_block_map",
                [this](inode_id_t id) { return this->get_block_map(id); });
  server_->bind("alloc_block",
                [this](inode_id_t id) { return this->allocate_block(id); });
  server_->bind("free_block",
                [this](inode_id_t id, block_id_t block, mac_id_t machine_id) {
                  return this->free_block(id, block, machine_id);
                });
  server_->bind("readdir", [this](inode_id_t id) { return this->readdir(id); });
  server_->bind("get_type_attr",
                [this](inode_id_t id) { return this->get_type_attr(id); });
}

inline auto MetadataServer::init_fs(const std::string &data_path) {
  /**
   * Check whether the metadata exists or not.
   * If exists, we wouldn't create one from scratch.
   */
  bool is_initialed = is_file_exist(data_path);

  auto block_manager = std::shared_ptr<BlockManager>(nullptr);
  if (is_log_enabled_) {
    block_manager =
        std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
  } else {
    block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
  }

  CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

  if (is_initialed) {
    auto origin_res = FileOperation::create_from_raw(block_manager);
    std::cout << "Restarting..." << std::endl;
    if (origin_res.is_err()) {
      std::cerr << "Original FS is bad, please remove files manually."
                << std::endl;
      exit(1);
    }

    operation_ = origin_res.unwrap();
  } else {
    operation_ = std::make_shared<FileOperation>(block_manager,
                                                 DistributedMaxInodeSupported);
    std::cout << "We should init one new FS..." << std::endl;
    /**
     * If the filesystem on metadata server is not initialized, create
     * a root directory.
     */
    auto init_res = operation_->alloc_inode(InodeType::Directory);
       if (init_res.is_err()) {
      std::cerr << "Cannot allocate inode for root directory." << std::endl;
      exit(1);
    }
    CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
  }

    
  running = false;
  num_data_servers =
      0; // Default no data server. Need to call `reg_server` to add.

  if (is_log_enabled_) {
    if (may_failed_)
      operation_->block_manager_->set_may_fail(true);
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled_);
  }

  bind_handlers();
  /**
   * The metadata server wouldn't start immediately after construction.
   * It should be launched after all the data servers are registered.
   */
}

MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

MetadataServer::MetadataServer(std::string const &address, u16 port,
                               const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(address, port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

auto calculate_block_sz(u64 file_sz, u64 block_sz) -> u64;

auto MetadataServer::read_inode(inode_id_t id, std::vector<u8> &buffer)
    -> ChfsResult<block_id_t> {

  if (id >= this->operation_->inode_manager_->get_max_inode_supported() - 1) {
    return ChfsResult<block_id_t>(ErrorType::INVALID_ARG);
  }

  auto block_id = this->operation_->inode_manager_->get(id);
 // std::cout << "inode id:" << id << "block id:" << block_id.unwrap() << std::endl;

  if (block_id.is_err()) {

    return ChfsResult<block_id_t>(block_id.unwrap_error());
  }

  if (block_id.unwrap() == KInvalidBlockID) {
    return ChfsResult<block_id_t>(ErrorType::INVALID_ARG);
  }

  auto res = this->operation_->block_manager_->read_block(block_id.unwrap(), buffer.data());
  if (res.is_err()) {
    return ChfsResult<block_id_t>(res.unwrap_error());
  }
  return ChfsResult<block_id_t>(block_id.unwrap());
}

// {Your code here}
auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name)
    -> inode_id_t {  
      lock.lock();
 //     std::cout << "a mknode has begun" << std::endl;
  // TODO: Implement this function.
    std::list<DirectoryEntry> list;

  // TODO: Implement this function.

  read_directory(this->operation_.get(), parent, list);

  std::string name_string(name);
  for (auto it = list.begin(); it != list.end(); it++)
  {
    if ((*it).name == name_string)
    {
//  std::cout << "a mknode has finished" << std::endl;
      lock.unlock();
      return -1;
    }
  }

  std::vector<std::shared_ptr<BlockOperation>> ops;

//  std::cout << "begin allocate" << std::endl;
  block_id_t bid = this->operation_->block_allocator_->allocate().unwrap();
//  std::cout << "allocate bid success" << std::endl;
  inode_id_t ret_id = this->operation_->inode_manager_->allocate_inode((InodeType)type, bid, true).unwrap();
//  std::cout << "allocate inode success" << std::endl;

  std::string de_name(name);
  std::string src = dir_list_to_string(list);
  src = append_to_directory(src, de_name, ret_id);
  std::vector<u8> buffer(this->operation_->block_manager_->block_size());
  buffer.assign(src.begin(), src.end());
//  std::cout << "begin write file" << std::endl;
  this->operation_->write_file(parent, buffer);
//  std::cout << "write file success" << std::endl;

  if (is_log_enabled_)
  {
    commit_log->append_log(txn_id, this->operation_->block_manager_->ops);
    txn_id++;
    this->operation_->block_manager_->ops.clear();
  }


  if (may_failed_) return 0;  
 // std::cout << "a mknode has finished" << std::endl;
  lock.unlock();
  return ret_id;

}

// {Your code here}
auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
    -> bool {
  lock.lock();
//  std::cout << "an unlink has begun" << std::endl;
  // TODO: Implement this function.
    std::list<DirectoryEntry> list;
  inode_id_t child;
  read_directory(this->operation_.get(), parent, list);
  std::string src = dir_list_to_string(list);
  std::string name_string(name);
  bool found = false;
  for (auto it = list.begin(); it != list.end(); it++)
  {
//    std::cout << "name: " << (*it).name << " id: " << (*it).id << std::endl;
    if ((*it).name == name_string)
    {
      child = (*it).id;
      found = true;
    }
  }
  if (!found) 
  {  
//  std::cout << "an unlink has begun" << std::endl;
  lock.unlock();
    return false;
  }
  this->operation_->remove_file(child);
  src = rm_from_directory(src, name_string);
  std::vector<u8> buffer(this->operation_->block_manager_->block_size());
  buffer.assign(src.begin(), src.end()); 
//  std::cout << "begin write file in unlink" << std::endl;
  this->operation_->write_file(parent, buffer);
//  std::cout << "write file success" << std::endl;


  if (is_log_enabled_)
  {
    commit_log->append_log(txn_id, this->operation_->block_manager_->ops);
    txn_id++;
    this->operation_->block_manager_->ops.clear();
  }  
//  std::cout << "an unlink has finished" << std::endl;
  lock.unlock();
  return true;

  // if (res.unwrap() != KNullOk.unwrap()) return false;
  // return true;
}

// {Your code here}
auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // TODO: Implement this function.
    std::list<DirectoryEntry> list;
  read_directory(this->operation_.get(), parent, list);
  std::string name_string(name);
  for (auto it = list.begin(); it != list.end(); it++)
  {
//    std::cout << "name: " << (*it).name << " id: " << (*it).id << std::endl;
    if ((*it).name == name_string)
    {
      return (*it).id;
    }
  }

  return 0;
}

// {Your code here}
auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
  // TODO: Implement this function.
  std::vector<u8> inode(this->operation_->block_manager_->block_size());
  std::vector<u8> indirect_block(0);
  indirect_block.reserve(this->operation_->block_manager_->block_size());

  auto inode_p = reinterpret_cast<Inode *>(inode.data());
 // auto inlined_blocks_num = 0;

  read_inode(id, inode);

  auto original_file_sz = inode_p->get_size();
  auto old_block_num = calculate_block_sz(original_file_sz, this->operation_->block_manager_->block_size());
  
    std::vector<BlockInfo> infos(inode_p->blockInfos, inode_p->blockInfos + old_block_num);

  return infos;
}

// {Your code here}
auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
  // TODO: Implement this function.
  lock.lock();
//  std::cout << "begin allocate" << std::endl;
  mac_id_t clients_num = clients_.size();
  srand(time(0));
  mac_id_t machine_id = (rand() % (clients_num)) + 1;
  auto cli = clients_[machine_id];          
  auto res = cli->call("alloc_block");

  auto [block_id, version] = res.unwrap()->as<std::pair<block_id_t, version_t>>();

//std::cout << "called data serv" << std::endl;

  std::vector<u8> inode(this->operation_->block_manager_->block_size());
  std::vector<u8> indirect_block(0);
  indirect_block.reserve(this->operation_->block_manager_->block_size());

  auto inode_p = reinterpret_cast<Inode *>(inode.data());
//  auto inlined_blocks_num = 0;

//  std::cout << "begin read inode in allocate block" << std::endl;
  auto inode_res = read_inode(id, inode);

//  std::cout << "finish read inode in allocate block" << std::endl;

  auto original_file_sz = inode_p->get_size();
  auto old_block_num = calculate_block_sz(original_file_sz, this->operation_->block_manager_->block_size());
//  auto new_block_num = old_block_num + 1;

  inode_p->set_blockinfo(old_block_num, block_id, machine_id, version);
  // if (inode_p->is_direct_block(block_id))
  //       {
  //         inode_p->set_blockinfo(old_block_num, block_id, machine_id, version);
  // //        std::cout << "allocated direct:" << bid << std::endl;
  //       }
  //       else
  //       {
  //           block_id_t indirect = inode_p->get_or_insert_indirect_block(this->operation_->block_allocator_).unwrap();
  //           this->operation_->block_manager_->read_block(indirect, indirect_block.data());
  //           block_id_t *indirect_blockids = (block_id_t *)indirect_block.data();

  //           *(indirect_blockids + old_block_num - inode_p->get_direct_block_num()) = block_id;
  //           this->operation_->block_manager_->write_block(indirect, indirect_block.data());
  // //        std::cout << "allocated indirect:" << bid << "indirect number:" << idx - inode_p->get_direct_block_num() << std::endl;
  //       }

  inode_p->inner_attr.size = original_file_sz + this->operation_->block_manager_->block_size();
  inode_p->inner_attr.set_all_time(time(0));

    auto write_res =
        this->operation_->block_manager_->write_block(inode_res.unwrap(), inode.data());
    if (write_res.is_err()) {

      std::cerr << "metadata server allocate inode fail!"<< std::endl;
    }

//  std::cout << "allocate block " << block_id << "on machine " << machine_id << ", version " << version << std::endl;
lock.unlock();
  return BlockInfo(block_id, machine_id, version);
}

// {Your code here}
auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                mac_id_t machine_id) -> bool {
lock.lock();
  // TODO: Implement this function.

//  std::cout << "begin free block" << std::endl;
  auto cli = clients_[machine_id];
  auto res = cli->call("free_block", block_id);
  auto freed = res.unwrap()->as<bool>();

  std::vector<u8> inode(this->operation_->block_manager_->block_size());

  auto inode_p = reinterpret_cast<Inode *>(inode.data());
 // auto inlined_blocks_num = 0;
//std::cout << "begin read inode in free block"<< std::endl;
  auto inode_res = read_inode(id, inode);
//std::cout << "finish read inode in free block"<< std::endl;

  inode_p->inner_attr.size = inode_p->get_size() - this->operation_->block_manager_->block_size();
  inode_p->inner_attr.set_all_time(time(0));

  auto write_res =
      this->operation_->block_manager_->write_block(inode_res.unwrap(), inode.data());
  if (write_res.is_err()) {

    std::cerr << "metadata server free inode fail!"<< std::endl;
  }
lock.unlock();
  return freed;
}

// {Your code here}
auto MetadataServer::readdir(inode_id_t node)
    -> std::vector<std::pair<std::string, inode_id_t>> {
  // TODO: Implement this function.

  std::list<DirectoryEntry> list;
  chfs::read_directory(this->operation_.get(), node, list);
  std::vector<std::pair<std::string, inode_id_t>> dirvec;
  for (auto it = list.begin(); it != list.end(); it++)
  {
    std::pair<std::string, inode_id_t> temp;
    temp.first = (*it).name;
    temp.second = (*it).id;
    dirvec.push_back(temp);
  }
  return dirvec;
}

// {Your code here}
auto MetadataServer::get_type_attr(inode_id_t id)
    -> std::tuple<u64, u64, u64, u64, u8> {
  // TODO: Implement this function.

  std::vector<u8> inode(this->operation_->block_manager_->block_size());
  
  auto inode_p = reinterpret_cast<Inode *>(inode.data());
//  auto inlined_blocks_num = 0;

  read_inode(id, inode);
  auto temp = inode_p->get_attr();
  std::tuple<u64, u64, u64, u64, u8> res(temp.size, temp.atime, temp.mtime, temp.ctime, (u8) inode_p->get_type());
  return res;
}

auto MetadataServer::reg_server(const std::string &address, u16 port,
                                bool reliable) -> bool {
  num_data_servers += 1;
  auto cli = std::make_shared<RpcClient>(address, port, reliable);
  clients_.insert(std::make_pair(num_data_servers, cli));

  return true;
}

//data operations
auto MetadataServer::run() -> bool {
  if (running)
    return false;

  // Currently we only support async start
  server_->run(true, num_worker_threads);
  running = true;
  return true;
}


} // namespace chfs