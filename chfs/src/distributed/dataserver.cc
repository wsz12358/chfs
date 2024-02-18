#include "distributed/dataserver.h"
#include "common/util.h"

namespace chfs {

auto DataServer::initialize(std::string const &data_path) {
  /**
   * At first check whether the file exists or not.
   * If so, which means the distributed chfs has
   * already been initialized and can be rebuilt from
   * existing data.
   */
  bool is_initialized = is_file_exist(data_path);

  auto bm = std::shared_ptr<BlockManager>(
      new BlockManager(data_path, KDefaultBlockCnt));
  if (is_initialized) {
    block_allocator_ =
        std::make_shared<BlockAllocator>(bm, bool(true), false);
  } else {
    // We need to reserve some blocks for storing the version of each block
    block_allocator_ = std::shared_ptr<BlockAllocator>(
        new BlockAllocator(bm, bool(true), true));
  }

  // Initialize the RPC server and bind all handlers
  server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                    usize len, version_t version) {
    return this->read_data(block_id, offset, len, version);
  });
  server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                     std::vector<u8> &buffer) {
    return this->write_data(block_id, offset, buffer);
  });
  server_->bind("alloc_block", [this]() { return this->alloc_block(); });
  server_->bind("free_block", [this](block_id_t block_id) {
    return this->free_block(block_id);
  });

  // Launch the rpc server to listen for requests
  server_->run(true, num_worker_threads);
}

DataServer::DataServer(u16 port, const std::string &data_path)
    : server_(std::make_unique<RpcServer>(port)) {
  initialize(data_path);
}

DataServer::DataServer(std::string const &address, u16 port,
                       const std::string &data_path)
    : server_(std::make_unique<RpcServer>(address, port)) {
  initialize(data_path);
}

DataServer::~DataServer() { server_.reset(); }

// {Your code here}
auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                           version_t version) -> std::vector<u8> {
  // TODO: Implement this function.
  version_t pre_version = get_version(block_id);
  if (pre_version != version) 
  {
    std::vector<u8> buffer;
    return buffer;
  }
  
  std::vector<u8> buffer(block_allocator_->bm->block_size());
  block_allocator_->bm->read_block(block_id, buffer.data());
  std::vector<u8> res(len);

  for (int i = offset; i < offset + len; i++)
  {
    res[i - offset] = buffer[i];
//    std::cout << "read" << (char )buffer[i] << std::endl;
  }
  return res;
}

// {Your code here}
auto DataServer::write_data(block_id_t block_id, usize offset,
                            std::vector<u8> &buffer) -> bool {
  // TODO: Implement this function.''

  block_allocator_->bm->write_partial_block(block_id, buffer.data(), offset, buffer.size());

  return true;
}

// {Your code here}
auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
  // TODO: Implement this function.
  
  block_id_t block_id = block_allocator_->allocate().unwrap();
  version_t version = get_version(block_id);
  set_version(block_id, version + 1);
  return std::pair(block_id, version + 1);
}

// {Your code here}
auto DataServer::free_block(block_id_t block_id) -> bool {
  // TODO: Implement this function.
  
  auto res = block_allocator_->deallocate(block_id);
  if (res.is_err()) return false;
  version_t version = get_version(block_id);
  set_version(block_id, version + 1);
  return true;
}

auto DataServer::get_version(block_id_t block_id) -> version_t
{
  usize version_block_id = block_id * sizeof(version_t) / block_allocator_->bm->block_size();
  std::vector<u8> buffer(block_allocator_->bm->block_size());
  block_allocator_->bm->read_block(version_block_id, buffer.data());
  auto temp = (version_t *) buffer.data();
  return temp[block_id - version_block_id * block_allocator_->bm->block_size() / sizeof(version_t)];
}

auto DataServer::set_version(block_id_t block_id, version_t version) -> ChfsNullResult
{
  usize version_block_id = block_id * sizeof(version_t) / block_allocator_->bm->block_size();
  std::vector<u8> buffer(block_allocator_->bm->block_size());
  block_allocator_->bm->read_block(version_block_id, buffer.data());
  auto temp = (version_t *) buffer.data();
  temp[block_id - version_block_id * block_allocator_->bm->block_size() / sizeof(version_t)] = version;
  block_allocator_->bm->write_block(version_block_id, buffer.data());
  return KNullOk;
}
} // namespace chfs