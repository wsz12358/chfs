#include <string>
#include <utility>
#include <vector>
#include <algorithm>

#include "map_reduce/protocol.h"

namespace mapReduce {
    SequentialMapReduce::SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client,
                                             const std::vector<std::string> &files_, std::string resultFile) {
        chfs_client = std::move(client);
        files = files_;
        outPutFile = resultFile;
        // Your code goes here (optional)
    }

    void SequentialMapReduce::doWork() {
        // Your code goes here
        std::vector<KeyVal> keyvals;
        for (std::string file: files)
        {
            auto res_lookup = chfs_client->lookup(1, file);
            auto inode_id = res_lookup.unwrap();
            auto res_type = chfs_client->get_type_attr(inode_id);
            auto length = res_type.unwrap().second.size;
            auto res_read = chfs_client->read_file(inode_id,0,length);
            auto char_vec = res_read.unwrap();
            std::string content;
            content.assign(char_vec.begin(), char_vec.end());
            std::vector<KeyVal> retval = Map(content);
            keyvals.insert(keyvals.end(), retval.begin(), retval.end());
        }
        std::map<std::string, std::shared_ptr<std::vector<std::string>>> zipped_kv;
        for (auto keyval: keyvals)
        {
            if (zipped_kv.count(keyval.key) > 0)
            {
                zipped_kv[keyval.key]->push_back(keyval.val);
            }
            else
            {
                std::pair<std::string, std::shared_ptr<std::vector<std::string>>> pair;
                pair.first = keyval.key;
                pair.second = std::make_shared<std::vector<std::string>>();
                zipped_kv.insert(pair);
                zipped_kv[keyval.key]->push_back(keyval.val);
            }
        }
        std::string output = "";
        for (auto kv: zipped_kv)
        {

            auto res = Reduce(kv.first, *kv.second);
//            std::cout << "there are " << res << " of " << kv.first << "word" << std::endl;
            output += kv.first;
            output += " ";
            output += res;
            output += "\n";
        }
        for (int i = 0; i < 10000; i++){}
        std::vector<chfs::u8> data;
        data.assign(output.begin(), output.end());
        auto res_lookup = chfs_client->lookup(1, outPutFile);
        auto inode_id = res_lookup.unwrap();
        chfs_client->write_file(inode_id, 0, data);
    }
}