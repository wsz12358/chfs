#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>


#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>

#include "map_reduce/protocol.h"

namespace mapReduce {

    Worker::Worker(MR_CoordinatorConfig config) {
        mr_client = std::make_unique<chfs::RpcClient>(config.ip_address, config.port, true);
        outPutFile = config.resultFile;
        chfs_client = config.client;
        work_thread = std::make_unique<std::thread>(&Worker::doWork, this);
        // Lab4: Your code goes here (Optional).
    }

    void Worker::doMap(int index, const std::string &filename) {
        // Lab4: Your code goes here.
//        std::cout << "doing map " << index << std::endl;
        auto res_create = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, "mr-" + std::to_string(index));
        auto output_id = res_create.unwrap();
        auto res_lookup = chfs_client->lookup(1, filename);
        auto inode_id = res_lookup.unwrap();
        auto res_type = chfs_client->get_type_attr(inode_id);
        auto length = res_type.unwrap().second.size;
        auto res_read = chfs_client->read_file(inode_id,0,length);
        auto char_vec = res_read.unwrap();
        std::string content;
        content.assign(char_vec.begin(), char_vec.end());

        std::string output = "";
        std::map<std::string, int> kvmap;
        std::string temp;
        for (size_t i = 0; i < content.length(); ++i)
        {
            if (std::isalpha(content[i]))
            {
                temp += content[i];
            }
            else
            {
                if (temp != "") {
                    if (kvmap.count(temp) > 0) kvmap[temp]++;
                    else kvmap.insert(std::pair<std::string, int>(temp, 1));
//                    std::cout << "the word is " << temp << std::endl;
                }
                temp = "";
            }
        }
        for (auto it = kvmap.begin(); it != kvmap.end(); it++)
        {

            output += it->first;
            output += " ";
            output += std::to_string(it->second);
            output += "\n";
        }
        std::vector<uint8_t> vec;
        vec.assign(output.begin(), output.end());
        chfs_client->write_file(output_id, 0, vec);

 //       std::cout << "finish map " << index << std::endl;

    }

    void Worker::doReduce(int index, int nfiles) {
        // Lab4: Your code goes here.

//        std::cout << "doing reduce " << index << std::endl;
        std::map<std::string, int> kvmap;

        for (int i = 0; i < nfiles; i++)
        {
            auto res_lookup = chfs_client->lookup(1, "mr-" + std::to_string(i));
            auto inode_id = res_lookup.unwrap();
            auto res_type = chfs_client->get_type_attr(inode_id);
            auto length = res_type.unwrap().second.size;
            auto res_read = chfs_client->read_file(inode_id,0,length);
            auto char_vec = res_read.unwrap();
            std::string content(char_vec.begin(), char_vec.end());
            std::stringstream stringstream(content);
            std::string key, value;
            while (stringstream >> key >> value) {
                if(true) {
                    std::size_t first_not_null = key.find_first_not_of('\0');
                    std::string insert_key;
                    int insert_value;
                    if (first_not_null == std::string::npos) {
                        insert_key = "";
                        insert_value = std::atoi(value.c_str());
                    }
                    insert_key = key.substr(first_not_null);
                    insert_value = std::atoi(value.c_str());
                    if (kvmap.count(insert_key) > 0) kvmap[insert_key] += insert_value;
                    else {
                        kvmap.insert(std::pair<std::string, int>(insert_key, insert_value));
                    }
                }
            }
        }
        std::string output;
        for (auto it = kvmap.begin(); it != kvmap.end(); it++)
        {

            output += it->first;
            output += " ";
            output += std::to_string(it->second);
            output += "\n";
        }
//        std::cout << "reduce " << index << ":"<< output << std::endl;
        std::vector<chfs::u8> data;
        data.assign(output.begin(), output.end());
        auto res_lookup = chfs_client->lookup(1, outPutFile);
        auto inode_id = res_lookup.unwrap();
        auto res_type = chfs_client->get_type_attr(inode_id);
        auto length = res_type.unwrap().second.size;
        chfs_client->write_file(inode_id, length, data);
 //       std::cout << "finish reduce " << index << std::endl;
    }

    void Worker::doSubmit(int taskType, int index) {
        // Lab4: Your code goes here.
        mr_client->call(SUBMIT_TASK, taskType, index);
    }

    void Worker::stop() {
        shouldStop = true;
        work_thread->join();
    }

    void Worker::doWork() {
        while (!shouldStop) {
            // Lab4: Your code goes here.
            auto res = mr_client->call(ASK_TASK, 1);
            auto res_unwrap = res.unwrap()->as<std::tuple<int, int, std::string>>();
            if (std::get<1>(res_unwrap) == -1)
            {
                continue;
            }
            else
            {
                if (std::get<1>(res_unwrap) == 1)
                {
                    doMap(std::get<0>(res_unwrap), std::get<2>(res_unwrap));
                    doSubmit(std::get<1>(res_unwrap), std::get<0>(res_unwrap));
                }
                else if (std::get<1>(res_unwrap) == 2)
                {
                    doReduce(std::get<0>(res_unwrap), std::atoi(std::get<2>(res_unwrap).c_str()));
                    doSubmit(std::get<1>(res_unwrap), std::get<0>(res_unwrap));
                }
            }

        }
    }
}