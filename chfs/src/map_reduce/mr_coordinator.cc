#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <mutex>

#include "map_reduce/protocol.h"

namespace mapReduce {
    std::tuple<int, int, std::string> Coordinator::askTask(int) {
        // Lab4 : Your code goes here.
        // Free to change the type of return value.
        mtx.lock();
        if (!mapped)
        {
            if (files_mapped != files.size()) {
                int this_map = files_mapped;
                files_mapped++;
 //               std::cout << "mapping " << this_map << std::endl;
                mtx.unlock();
                return std::make_tuple(this_map, 1,files[this_map]);
            }
            else
            {
                mtx.unlock();
                return std::make_tuple(-1, -1,"");
            }

        }
        else
        {
            if (files_reduced != n_reduce) {
                int this_reduce = files_reduced;
                files_reduced++;
 //               std::cout << "reducing " << this_reduce << std::endl;
                mtx.unlock();
                return std::make_tuple(this_reduce, 2,std::to_string(files.size()));
            }
            else
            {
                mtx.unlock();
                return std::make_tuple(-1, -1,"");
            }

        }
        mtx.unlock();
        return std::make_tuple(-1, -1,"");
    }

    int Coordinator::submitTask(int taskType, int index) {
        // Lab4 : Your code goes here.
        mtx.lock();
        if (taskType == 1)
        {
//            std::cout << "map finish " << map_submitted << std::endl;
            map_submitted++;
            if (map_submitted == files.size())
            {
//                std::cout << "map finish " << std::endl;
                mapped = true;
            }
            mtx.unlock();
            return 1;
        }
        else if (taskType == 2)
        {

 //           std::cout << "reduce finish " << reduce_submitted << std::endl;
            reduce_submitted++;
            if (reduce_submitted == n_reduce)
            {
 //               std::cout << "reduce finish " << std::endl;
                this->isFinished = true;
            }
            mtx.unlock();
            return 1;
        }
        mtx.unlock();
        return 0;
    }

    // mr_coordinator calls Done() periodically to find out
    // if the entire job has finished.
    bool Coordinator::Done() {
        std::unique_lock<std::mutex> uniqueLock(this->mtx);
        return this->isFinished;
    }

    // create a Coordinator.
    // nReduce is the number of reduce tasks to use.
    Coordinator::Coordinator(MR_CoordinatorConfig config, const std::vector<std::string> &files, int nReduce) {
        this->files = files;
        this->isFinished = false;
        // Lab4: Your code goes here (Optional).
        n_reduce = 1;
    
        rpc_server = std::make_unique<chfs::RpcServer>(config.ip_address, config.port);
        rpc_server->bind(ASK_TASK, [this](int i) { return this->askTask(i); });
        rpc_server->bind(SUBMIT_TASK, [this](int taskType, int index) { return this->submitTask(taskType, index); });
        rpc_server->run(true, 1);
    }
}