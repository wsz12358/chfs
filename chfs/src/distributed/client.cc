#include "distributed/client.h"
#include "common/macros.h"
#include "common/util.h"
#include "distributed/metadata_server.h"

namespace chfs {

ChfsClient::ChfsClient() : num_data_servers(0) {}

auto ChfsClient::reg_server(ServerType type, const std::string &address,
                            u16 port, bool reliable) -> ChfsNullResult {
  switch (type) {
  case ServerType::DATA_SERVER:
    num_data_servers += 1;
    data_servers_.insert({num_data_servers, std::make_shared<RpcClient>(
                                                address, port, reliable)});
    break;
  case ServerType::METADATA_SERVER:
    metadata_server_ = std::make_shared<RpcClient>(address, port, reliable);
    break;    
  default:
    std::cerr << "Unknown Type" << std::endl;
    exit(1);
  }

  return KNullOk;
}

auto calculate_block_sz(u64 file_sz, u64 block_sz) -> u64;

// {Your code here}
auto ChfsClient::mknode(FileType type, inode_id_t parent,
                        const std::string &name) -> ChfsResult<inode_id_t> {
  // TODO: Implement this function.
  auto res = metadata_server_->call("mknode", (u8) type, parent, name);
  auto inode_id = res.unwrap()->as<inode_id_t>();
  if (inode_id == -1) return ErrorType::INVALID;
  return ChfsResult<inode_id_t>(inode_id);
}

// {Your code here}
auto ChfsClient::unlink(inode_id_t parent, std::string const &name)
    -> ChfsNullResult {
  // TODO: Implement this function.
  auto res = metadata_server_->call("unlink", parent, name);
  bool un_res = res.unwrap()->as<bool>();
  if (un_res) return KNullOk;
  else return ErrorType::INVALID;
}

// {Your code here}
auto ChfsClient::lookup(inode_id_t parent, const std::string &name)
    -> ChfsResult<inode_id_t> {
  // TODO: Implement this function.
  auto res = metadata_server_->call("lookup", parent, name);
  auto inode_id = res.unwrap()->as<inode_id_t>();
  return ChfsResult<inode_id_t>(inode_id);
}

// {Your code here}
auto ChfsClient::readdir(inode_id_t id)
    -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>> {
  // TODO: Implement this function.
  auto res = metadata_server_->call("readdir", id);
  auto inode_id = res.unwrap()->as<std::vector<std::pair<std::string, inode_id_t>>>();
  return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(inode_id);
}

// {Your code here}
auto ChfsClient::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  // TODO: Implement this function.
  auto res = metadata_server_->call("get_type_attr", id);
  auto wrapped_res = res.unwrap()->as<std::tuple<u64, u64, u64, u64, u8>>();
  auto size = std::get<0>(wrapped_res);
  auto atime = std::get<1>(wrapped_res);
  auto mtime = std::get<2>(wrapped_res);
  auto ctime = std::get<3>(wrapped_res);
  auto type = (InodeType) std::get<4>(wrapped_res);
  FileAttr attr;
  attr.size = size;
  attr.atime = atime;
  attr.mtime = mtime;
  attr.ctime = ctime;
  return ChfsResult<std::pair<InodeType, FileAttr>>({type, attr});
}

/**
 * Read and Write operations are more complicated.
 */
// {Your code here}
auto ChfsClient::read_file(inode_id_t id, usize offset, usize size)
    -> ChfsResult<std::vector<u8>> {
  // TODO: Implement this function.
//  auto error_code = ErrorType::DONE;
  std::vector<u8> content(size);
  const auto block_size = chfs::DiskBlockSize;

  u64 read_sz = 0;


  auto res = metadata_server_->call("get_block_map", id);
  auto infos = res.unwrap()->as<std::vector<BlockInfo>>();

  int idx = offset / block_size;
  if (offset % block_size != 0)
  {
    BlockInfo info = infos[idx];
    u64 cur_sz = block_size - offset % block_size > size ? size : block_size - offset % block_size;
    auto wrapped_read_data = data_servers_[std::get<1>(info)]->call("read_data", 
        std::get<0>(info), offset % block_size, cur_sz, std::get<2>(info));
    auto unwrapped_data = wrapped_read_data.unwrap()->as<std::vector<u8>>();
    memcpy(content.data(), unwrapped_data.data(), cur_sz);
        std::cout << "read " << (char )unwrapped_data[0] << "from "<< std::get<0>(info) << std::endl;
    read_sz += cur_sz;
    idx++;
  }

  // Now read the file
  while (read_sz < size) {
    auto sz = ((size - read_sz) > block_size)
                  ? block_size
                  : (size - read_sz);

    BlockInfo info = infos[idx];
    auto wrapped_read_data = data_servers_[std::get<1>(info)]->call("read_data", 
        std::get<0>(info), 0, sz, std::get<2>(info));

    auto unwrapped_data = wrapped_read_data.unwrap()->as<std::vector<u8>>();
    memcpy(content.data() + read_sz, unwrapped_data.data(), sz);
 //   std::cout << "read " << (char )unwrapped_data[0] << "from "<< std::get<0>(info) << std::endl;
    
    read_sz += sz;
    idx++;
  }
  return ChfsResult<std::vector<u8>>(std::move(content));

// err_ret:
//   return ChfsResult<std::vector<u8>>(error_code);
//   return ChfsResult<std::vector<u8>>({});
}

// {Your code here}
auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data)
    -> ChfsNullResult {
  // TODO: Implement this function.
 //   auto error_code = ErrorType::DONE;
  const auto block_size = chfs::DiskBlockSize;
  usize old_block_num = 0;
  usize new_block_num = 0;
  u64 original_file_sz = 0;
//    bool need_indirect = false;

  // 1. read the inode

  // std::vector<u8> inode(block_size);

//   auto inode_p = reinterpret_cast<Inode *>(inode.data());
//   auto inlined_blocks_num = 0;

//   auto inode_res = this->inode_manager_->read_inode(id, inode);
//   if (inode_res.is_err()) {
//     error_code = inode_res.unwrap_error();
//     // I know goto is bad, but we have no choice
//     // goto err_ret;
//   } else {
//     inlined_blocks_num = inode_p->get_direct_block_num();
//   }

//   if (data.size() > inode_p->max_file_sz_supported()) {
//     std::cerr << "file size too large: " << data.size() << " vs. "
//               << inode_p->max_file_sz_supported() << std::endl;
//     error_code = ErrorType::OUT_OF_RESOURCE;
// //    goto err_ret;
//   }

  // 2. make sure whether we need to allocate more blocks
  auto fileattr = get_type_attr(id);
  original_file_sz = fileattr.unwrap().second.size;
  old_block_num = calculate_block_sz(original_file_sz, block_size);
  new_block_num = calculate_block_sz(data.size() + offset, block_size);
 // std::cout << "original:" << old_block_num << std::endl;
 // std::cout << "new:" << new_block_num << std::endl;

  if (new_block_num > old_block_num) {
    // If we need to allocate more blocks.

    for (usize idx = old_block_num; idx < new_block_num; ++idx) {
      auto res = metadata_server_->call("alloc_block", id);
    //auto infos = res.unwrap()->as<BlockInfo>();
    //std::cout <<
    }

  } else {
    // We need to free the extra blocks.
    auto res = metadata_server_->call("get_block_map", id);
    auto infos = res.unwrap()->as<std::vector<BlockInfo>>();
    for (usize idx = new_block_num; idx < old_block_num; ++idx) {
      BlockInfo info = infos[idx];
      auto res = metadata_server_->call("free_block", std::get<0>(info), std::get<1>(info));
    }
    }

//  auto block_idx = 0;
  u64 write_sz = 0;


  auto res = metadata_server_->call("get_block_map", id);
  auto infos = res.unwrap()->as<std::vector<BlockInfo>>();

  int idx = offset / block_size;
  if (offset % block_size != 0)
  {
    BlockInfo info = infos[idx];
    u64 cur_sz = block_size - offset % block_size > data.size() ? data.size() : block_size - offset % block_size;
    std::vector<u8> buffer(cur_sz);
    memcpy(buffer.data(), data.data(), cur_sz);
    data_servers_[std::get<1>(info)]->call("write_data", 
        std::get<0>(info), offset % block_size, buffer);

    write_sz += block_size - offset % block_size;
idx++;
  }
  while (write_sz < data.size()) {
    auto sz = ((data.size() - write_sz) > block_size)
                  ? block_size
                  : (data.size() - write_sz);
    std::vector<u8> buffer(sz);
    memcpy(buffer.data(), data.data() + write_sz, sz);
    // TODO: Write to current block.
    BlockInfo info = infos[idx];
    data_servers_[std::get<1>(info)]->call("write_data", std::get<0>(info), 0, buffer);
    write_sz += sz;
    idx += 1;
  }

  return KNullOk;

// err_ret:
//  std::cerr << "write file return error: " << (int)error_code << std::endl;
//   return ChfsNullResult(error_code);    
//   return KNullOk;
}

// {Your code here}
auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id,
                                 mac_id_t mac_id) -> ChfsNullResult {
  // TODO: Implement this function.
  metadata_server_->call("free_block", id, block_id, mac_id);
  return KNullOk;
}


} // namespace chfs