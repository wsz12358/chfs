#pragma once

#include "common/macros.h"
#include "block/manager.h"
#include <mutex>
#include <vector>
#include <cstring>

namespace chfs {

/** 
 * RaftLog uses a BlockManager to manage the data..
 */
template <typename Command>
class RaftLogContent{

};

template <typename Command>
class RaftLog {
public:
    RaftLog(std::shared_ptr<BlockManager> bm, int _id = -1);
    ~RaftLog();

    /* Lab3: Your code here */
    void SaveCommand(Command command,int term,int index);
    int GetSize();
    void SaveSize();
    int GetLastTerm();
    int GetLastIndex();
    int GetTerm(int index);
    int total_sz = 0;
    void SaveSuperBlock(int, int, int);
    void SaveSnapshot(std::vector<u8> data, int index, int term);

    int voted_for = -1;
    int current_term = 0;
    int id = -1;
    int snapshot_index = 0;
    int snapshot_term = -1;
    int snapshot_size = 0;
    int installed_snapshot = 0;
    std::vector<Command> commands;
    std::vector<int> terms;
    std::vector<int> indexs;
    std::vector<u8> snapshot;

private:
    std::shared_ptr<BlockManager> bm_;
    std::mutex mtx;
    /* Lab3: Your code here */

};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm, int _id)
{
    mtx.lock();
    /* Lab3: Your code here */
    bm_ = bm;
    id = _id;
    std::vector<u8> super(bm_->block_size());
    bm_->read_block(0, super.data());
    int *ptr = (int *)super.data();
    total_sz =  *ptr;
    current_term = *(ptr + 1);
    voted_for = *(ptr + 2);
    installed_snapshot = *(ptr + 3);
    snapshot_index = *(ptr + 4);
    snapshot_term = *(ptr + 5);
    snapshot_size = *(ptr + 6);
    if (snapshot_size != 0)
    {
        std::vector<u8> temp(bm_->block_size());
        snapshot = temp;
        std::vector<u8> ss_block(bm_->block_size());
        bm_->read_block(1, ss_block.data());
        memcpy(snapshot.data(), ss_block.data(), snapshot_size);
    }
    if (total_sz != 0)
    {
//        std::cout << "log " << id << " read super:" << total_sz << " " << current_term << " " << voted_for << std::endl;
        for (int i = snapshot_index + 2; i < snapshot_index + total_sz + 2; i++)
        {
            std::vector<u8> tmp_block(bm_->block_size());
            bm_->read_block(i, tmp_block.data());
            int *block_ptr = (int *)tmp_block.data();
            commands.push_back(Command(*(block_ptr)));
            terms.push_back(*(block_ptr + 1));
            indexs.push_back(*(block_ptr + 2));
//            std::cout << "log " << id << " read command:" << *(block_ptr) << " " << *(block_ptr + 1) << " " << *(block_ptr + 2) << std::endl;
        }
    }
    mtx.unlock();
}

template <typename Command>
int RaftLog<Command>::GetSize()
{
    std::vector<u8> super(bm_->block_size());
    bm_->read_block(0, super.data());
    return *((int *)super.data());
}

template <typename Command>
void RaftLog<Command>::SaveSnapshot(std::vector<u8> data, int index, int term)
{
    std::vector<u8> ss_block(bm_->block_size());
    memcpy(ss_block.data(), data.data(), data.size());
    bm_->write_block(1, ss_block.data());
    snapshot = data;
    snapshot_index = index;
    snapshot_term = term;
    snapshot_size = data.size();
    installed_snapshot = 1;
}


template <typename Command>
void RaftLog<Command>::SaveSuperBlock(int size, int current_term_, int voted_for_)
{
    mtx.lock();
//    std::cout << "log " << id << " set super:" << size << " " << current_term_ << " " << voted_for_ << std::endl;
    std::vector<u8> super(bm_->block_size());
    int *ptr = (int *)super.data();
    *ptr = size;
    *(ptr + 1) = current_term_;
    *(ptr + 2) = voted_for_;
    *(ptr + 3) = installed_snapshot;
    *(ptr + 4) = snapshot_index;
    *(ptr + 5) = snapshot_term;
    *(ptr + 6) = snapshot_size;
    total_sz = size;
    current_term = current_term_;
    voted_for = voted_for_;
    bm_->write_block(0, super.data());
    mtx.unlock();
}

template <typename Command>
void RaftLog<Command>::SaveCommand(Command command, int term, int index) {
    mtx.lock();
//    std::cout << "log " << id << " set command:" << command.value << " " << term << " " << index << std::endl;
    std::vector<u8> block(bm_->block_size());
    int *block_ptr = (int *) block.data();
    *block_ptr = command.value;
    *(block_ptr + 1) = term;
    *(block_ptr + 2) = index;
    bm_->write_block(index + 1, block.data());
    total_sz++;
    int size = total_sz, current_term_ = current_term, voted_for_ = voted_for;
//    std::cout << "log " << id << " set super:" << size << " " << current_term << " " << voted_for_ << std::endl;
    std::vector<u8> super(bm_->block_size());
    int *ptr = (int *)super.data();
    *ptr = size;
    *(ptr + 1) = current_term_;
    *(ptr + 2) = voted_for_;
    *(ptr + 3) = installed_snapshot;
    total_sz = size;
    current_term = current_term_;
    voted_for = voted_for_;
    bm_->write_block(0, super.data());
    mtx.unlock();
}

template <typename Command>
int RaftLog<Command>::GetLastIndex()
{
    return total_sz;
}

template <typename Command>
int RaftLog<Command>::GetLastTerm()
{
    return GetTerm(total_sz);
}

template <typename Command>
int RaftLog<Command>::GetTerm(int index)
{
    std::vector<u8> block(bm_->block_size());
    bm_->read_block(index, block.data());
    return *((int *)(block.data()) + 1);
}

template <typename Command>
RaftLog<Command>::~RaftLog()
{
    /* Lab3: Your code here */
}

/* Lab3: Your code here */

} /* namespace chfs */
