#include <algorithm>
#include <sstream>
#include "filesystem/directory_op.h"

namespace chfs {

/**
 * Some helper functions
 */
auto string_to_inode_id(std::string &data) -> inode_id_t {
  std::stringstream ss(data);
  inode_id_t inode;
  ss >> inode;
  return inode;
}

auto inode_id_to_string(inode_id_t id) -> std::string {
  std::stringstream ss;
  ss << id;
  return ss.str();
}

// {Your code here}
auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
    -> std::string {
  std::ostringstream oss;
  usize cnt = 0;
  for (const auto &entry : entries) {
    oss << entry.name << ':' << entry.id;
    if (cnt < entries.size() - 1) {
      oss << '/';
    }
    cnt += 1;
  }
  return oss.str();
}

// {Your code here}
auto append_to_directory(std::string src, std::string filename, inode_id_t id)
    -> std::string {

  // TODO: Implement this function.
  //       Append the new directory entry to `src`.
  std::list<DirectoryEntry> list;
  parse_directory(src, list);
  DirectoryEntry de;
  de.id = id;
  de.name = filename;
  list.push_back(de);
  src = dir_list_to_string(list);
  return src;
}

// {Your code here}
void parse_directory(std::string &src, std::list<DirectoryEntry> &list) {

  // TODO: Implement this function.

  std::istringstream iss(src);
  std::string token;			// 接收缓冲区
  while (getline(iss, token, '/'))	// 以split为分隔符
  {
	std::istringstream iss2(token);
	std::string temp;
	DirectoryEntry de;
	getline(iss2, temp, ':');
	de.name = temp;
	getline(iss2, temp, ':');
	de.id = atoi(temp.c_str());
	list.push_back(de);
  }


}

// {Your code here}
auto rm_from_directory(std::string src, std::string filename) -> std::string {

  auto res = std::string("");

  // TODO: Implement this function.
  //       Remove the directory entry from `src`.
  std::list<DirectoryEntry> list;
  parse_directory(src, list);
  for (auto iter = list.begin(); iter != list.end(); iter++)
  {
    if ((*iter).name == filename)
    {
      list.erase(iter);
      res = dir_list_to_string(list);
      return res;
    }
  }
  res = dir_list_to_string(list);
  return res;
}

/**
 * { Your implementation here }
 */
auto read_directory(FileOperation *fs, inode_id_t id,
                    std::list<DirectoryEntry> &list) -> ChfsNullResult {
  
  // TODO: Implement this function.

  auto content = fs->read_file(id).unwrap();
  std::string src(content.begin(), content.end());
  parse_directory(src, list);
  return KNullOk;
}

// {Your code here}
auto FileOperation::lookup(inode_id_t id, const char *name)
    -> ChfsResult<inode_id_t> {
  std::list<DirectoryEntry> list;
  FileOperation fs(this->block_manager_, this->inode_manager_->get_max_inode_supported());

  // TODO: Implement this function.
  read_directory(&fs, id, list);
  std::string name_string(name);
  for (auto it = list.begin(); it != list.end(); it++)
  {
    // std::cout << "name is :" << (*it).name << ", id is " << (*it).id << std::endl;
    if ((*it).name == name_string)
    {
      return ChfsResult<inode_id_t>((*it).id);
    }
  }

  return ChfsResult<inode_id_t>(ErrorType::NotExist);
}

// {Your code here}
auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type)
    -> ChfsResult<inode_id_t> {

  // TODO:
  // 1. Check if `name` already exists in the parent.
  //    If already exist, return ErrorType::AlreadyExist.
  // 2. Create the new inode.
  // 3. Append the new entry to the parent directory.
  std::list<DirectoryEntry> list;

  // TODO: Implement this function.
  read_directory(this, id, list);
  std::string name_string(name);
  for (auto it = list.begin(); it != list.end(); it++)
  {
    if ((*it).name == name_string)
    {
      return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
    }
  }
  block_id_t bid = this->block_allocator_->allocate().unwrap();
  inode_id_t ret_id = this->inode_manager_->allocate_inode(type, bid).unwrap();
  std::string de_name(name);
  std::string src = dir_list_to_string(list);
  src = append_to_directory(src, de_name, ret_id);
  std::vector<u8> buffer(this->block_manager_->block_size());
  buffer.assign(src.begin(), src.end());
  this->write_file(id, buffer);
  return ChfsResult<inode_id_t>(static_cast<inode_id_t>(ret_id));
}

// {Your code here}
auto FileOperation::unlink(inode_id_t parent, const char *name)
    -> ChfsNullResult {

  // TODO: 
  // 1. Remove the file, you can use the function `remove_file`
  // 2. Remove the entry from the directory.
  std::list<DirectoryEntry> list;
  inode_id_t child;
  read_directory(this, parent, list);
  std::string src = dir_list_to_string(list);
  std::string name_string(name);
  bool found = false;
  for (auto it = list.begin(); it != list.end(); it++)
  {
    if ((*it).name == name_string)
    {
      child = (*it).id;
      found = true;
    }
  }
  if (!found) return KNullOk;
  remove_file(child);
  std::cerr << src << std::endl;
  src = rm_from_directory(src, name_string);
  std::cerr << src << std::endl;
  std::vector<u8> buffer(this->block_manager_->block_size());
  buffer.assign(src.begin(), src.end());
  this->write_file(parent, buffer);

  return KNullOk;
}

} // namespace chfs
