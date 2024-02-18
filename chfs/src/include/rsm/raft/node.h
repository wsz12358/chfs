#pragma once

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <memory>
#include <stdarg.h>
#include <unistd.h>
#include <filesystem>
#include <sys/time.h>

#include "rsm/state_machine.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "utils/thread_pool.h"
#include "librpc/server.h"
#include "librpc/client.h"
#include "block/manager.h"

namespace chfs {

enum class RaftRole {
    Follower,
    Candidate,
    Leader
};

struct RaftNodeConfig {
    int node_id;
    uint16_t port;
    std::string ip_address;
};

template <typename StateMachine, typename Command>
class RaftNode {

#define RAFT_LOG(fmt, args...)                                                                                   \
    do {                                                                                                         \
        auto now =                                                                                               \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                std::chrono::system_clock::now().time_since_epoch())                                             \
                .count();                                                                                        \
        char buf[512];                                                                                      \
        sprintf(buf,"[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, role, ##args); \
        thread_pool->enqueue([=]() { std::cerr << buf;} );                                         \
    } while (0);

public:
    RaftNode (int node_id, std::vector<RaftNodeConfig> node_configs);
    ~RaftNode();

    /* interfaces for test */
    void set_network(std::map<int, bool> &network_availablility);
    void set_reliable(bool flag);
    int get_list_state_log_num();
    int rpc_count();
    std::vector<u8> get_snapshot_direct();

private:
    /* 
     * Start the raft node.
     * Please make sure all of the rpc request handlers have been registered before this method.
     */
    auto start() -> int;
    /*
     * Stop the raft node.
     */
    auto stop() -> int;
    
    /* Returns whether this node is the leader, you should also return the current term. */
    auto is_leader() -> std::tuple<bool, int>;

    /* Checks whether the node is stopped */
    auto is_stopped() -> bool;

    /* 
     * Send a new command to the raft nodes.
     * The returned tuple of the method contains three values:
     * 1. bool:  True if this raft node is the leader that successfully appends the log,
     *      false If this node is not the leader.
     * 2. int: Current term.
     * 3. int: Log index.
     */
    auto new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>;

    /* Save a snapshot of the state machine and compact the log. */
    auto save_snapshot() -> bool;

    /* Get a snapshot of the state machine */
    auto get_snapshot() -> std::vector<u8>;


    /* Internal RPC handlers */
    auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;
    auto append_entries(RpcAppendEntriesArgs arg) -> AppendEntriesReply;
    auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

    /* RPC helpers */
    void send_request_vote(int target, RequestVoteArgs arg);
    void handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply);

    void send_append_entries(int target, AppendEntriesArgs<Command> arg);
    void handle_append_entries_reply(int target, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply);

    void send_install_snapshot(int target, InstallSnapshotArgs arg);
    void handle_install_snapshot_reply(int target, const InstallSnapshotArgs arg, const InstallSnapshotReply reply);

    /* background workers */
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();


    /* Data structures */
    bool network_stat;          /* for test */

    std::mutex mtx;                             /* A big lock to protect the whole data structure. */
    std::mutex clients_mtx;                     /* A lock to protect RpcClient pointers */
    std::unique_ptr<ThreadPool> thread_pool;
    std::unique_ptr<RaftLog<Command>> log_storage;     /* To persist the raft log. */
    std::unique_ptr<StateMachine> state;  /*  The state machine that applies the raft log, e.g. a kv store. */

    std::unique_ptr<RpcServer> rpc_server;      /* RPC server to recieve and handle the RPC requests. */
    std::map<int, std::unique_ptr<RpcClient>> rpc_clients_map;  /* RPC clients of all raft nodes including this node. */
    std::vector<RaftNodeConfig> node_configs;   /* Configuration for all nodes */ 
    int my_id;                                  /* The index of this node in rpc_clients, start from 0. */

    std::atomic_bool stopped;

    RaftRole role;
    int current_term;
    int leader_id;

    std::unique_ptr<std::thread> background_election;
    std::unique_ptr<std::thread> background_ping;
    std::unique_ptr<std::thread> background_commit;
    std::unique_ptr<std::thread> background_apply;

    /* Lab3: Your code here */
    int vote_for = -1;
    int accepted_followers = 0;
    int commit_index = 0;
    int last_applied = 0;
    std::vector<int> next_index;
    std::vector<int> match_index;
    std::vector<Command> commands;
    std::vector<int> terms;
    std::vector<int> indexs;
    long long last_rpc_time = 0;
    void send_heartbeat();
    int random_timeout();
    void set_rpc_time();
};

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::RaftNode(int node_id, std::vector<RaftNodeConfig> configs):
    network_stat(true),
    node_configs(configs),
    my_id(node_id),
    stopped(true),
    role(RaftRole::Follower),
    current_term(0),
    leader_id(-1)
{
    auto my_config = node_configs[my_id];
    thread_pool = std::make_unique<ThreadPool>(32);

    /* launch RPC server */
    rpc_server = std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

    /* Register the RPCs. */
    rpc_server->bind(RAFT_RPC_START_NODE, [this]() { return this->start(); });
    rpc_server->bind(RAFT_RPC_STOP_NODE, [this]() { return this->stop(); });
    rpc_server->bind(RAFT_RPC_CHECK_LEADER, [this]() { return this->is_leader(); });
    rpc_server->bind(RAFT_RPC_IS_STOPPED, [this]() { return this->is_stopped(); });
    rpc_server->bind(RAFT_RPC_NEW_COMMEND, [this](std::vector<u8> data, int cmd_size) { return this->new_command(data, cmd_size); });
    rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT, [this]() { return this->save_snapshot(); });
    rpc_server->bind(RAFT_RPC_GET_SNAPSHOT, [this]() { return this->get_snapshot(); });

    rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg) { return this->request_vote(arg); });
    rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg) { return this->append_entries(arg); });
    rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT, [this](InstallSnapshotArgs arg) { return this->install_snapshot(arg); });

   /* Lab3: Your code here */
   std::string file_name = "/tmp/raft_log/bm" + std::to_string(my_id);
   std::shared_ptr<BlockManager> bm = std::shared_ptr<BlockManager>(new BlockManager(file_name));
   log_storage = std::make_unique<RaftLog<Command>>(bm, my_id);
   state = std::make_unique<StateMachine>();
    if (log_storage->installed_snapshot == 1) state->apply_snapshot(log_storage->snapshot);
    commands = log_storage->commands;
    terms = log_storage->terms;
    indexs = log_storage->indexs;
    current_term = log_storage->current_term;
    vote_for = log_storage->voted_for;
    last_applied = log_storage->snapshot_index;
    commit_index = log_storage->snapshot_index;
    commands.insert(commands.begin(), Command(0));
    terms.insert(terms.begin(), log_storage->snapshot_term);
    indexs.insert(indexs.begin(), log_storage->snapshot_index);
    for (int i = 0; i < (int)configs.size(); i++) {
        next_index.push_back(1);
        match_index.push_back(0);
   }


    rpc_server->run(true, configs.size()); 
}

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::~RaftNode()
{
    stop();

    thread_pool.reset();
    rpc_server.reset();
    state.reset();
    log_storage.reset();
}

/******************************************************************

                        RPC Interfaces

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::start() -> int
{
    /* Lab3: Your code here */

    for (int i = 0; i < (int)node_configs.size(); i++) {
        rpc_clients_map.insert(std::pair<int, std::unique_ptr<RpcClient>>(i, std::make_unique<RpcClient>(node_configs[i].ip_address, node_configs[i].port, true)));
    }

    stopped = false;
    std::cerr << "started" << std::endl;
    background_election = std::make_unique<std::thread>(&RaftNode::run_background_election, this);
    background_ping = std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
    background_commit = std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
    background_apply = std::make_unique<std::thread>(&RaftNode::run_background_apply, this);

    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::stop() -> int
{
    /* Lab3: Your code here */
    stopped = true;
    background_election->join();
    background_ping->join();
    background_commit->join();
    background_apply->join();
    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int>
{
    /* Lab3: Your code here */
    if (role == RaftRole::Leader) return std::make_tuple(true, current_term);
    return std::make_tuple(false, current_term);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_stopped() -> bool
{
    return stopped.load();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>
{
    /* Lab3: Your code here */
    mtx.lock();
    if (role != RaftRole::Leader) {
        mtx.unlock();
        return std::make_tuple(false, -1, -1);
    }
    if (rpc_clients_map[my_id].get() == nullptr) {
//                current_term = 0;
        mtx.unlock();
        return std::make_tuple(false, -1, -1);
    }
    Command cmd;
    cmd.deserialize(cmd_data, cmd_size);
    commands.push_back(cmd);
    terms.push_back(current_term);
    indexs.push_back(commands.size() - 1 + log_storage->snapshot_index);
    log_storage->SaveCommand(cmd, current_term, commands.size() - 1 + log_storage->snapshot_index);
//    std::cout << my_id << " new command " << cmd.value << " at term " << current_term << " index " << commands.size() - 1 << std::endl;
    mtx.unlock();
    return std::make_tuple(true, current_term, commands.size() - 1 + log_storage->snapshot_index);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::save_snapshot() -> bool
{
    /* Lab3: Your code here */
    mtx.lock();
    int snapshot_index = last_applied;
    std::vector<u8> snapshot_data = state->snapshot();
    for(int i = log_storage->snapshot_index; i < snapshot_index; i++) {
        commands.erase(commands.begin());
        terms.erase(terms.begin());
        indexs.erase(indexs.begin());
    }
    log_storage->SaveSnapshot(snapshot_data, snapshot_index, terms[0]);
    log_storage->SaveSuperBlock(commands.size() - 1, current_term, vote_for);
    mtx.unlock();
    return true;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8>
{
    /* Lab3: Your code here */
    return state->snapshot();
}

/******************************************************************

                         Internal RPC Related

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args) -> RequestVoteReply
{
    mtx.lock();
//    std::cerr << "arg the term " << args.term << "my the term " << current_term << std::endl << std::flush;
    if (args.term >= current_term) {
        if (args.term > current_term) {
            role = RaftRole::Follower;
            current_term = args.term;
            vote_for = -1;
            accepted_followers = 0;
            log_storage->SaveSuperBlock(commands.size() - 1, current_term, vote_for);
            set_rpc_time();
        }
        /* Lab3: Your code here */
        if (vote_for == -1 || vote_for == args.candidate_id) {
            if (args.lastLogTerm > terms.back() ||
                (args.lastLogTerm == terms.back() && args.lastLogIndex >= indexs.back())) {
                RequestVoteReply reply;
                role = RaftRole::Follower;
                vote_for = args.candidate_id;
                reply.term = current_term;
                reply.voteGranted = true;
                log_storage->SaveSuperBlock(commands.size() - 1, current_term, vote_for);
                std::cerr << "I vote to " << args.candidate_id << ", I am " << my_id << std::endl << std::flush;
                mtx.unlock();
                return reply;
            }
        }
    }
    RequestVoteReply reply;
    reply.term = current_term;
    reply.voteGranted = false;
    std::cerr << "I veto to " << args.candidate_id << ", I am " << my_id << std::endl << std::flush;
    std::cerr << "arg term " << args.lastLogTerm << "my term " << terms.back() << std::endl << std::flush;
    std::cerr << "my vote " << vote_for << std::endl;
    std::cerr << "current_term " << current_term;
    mtx.unlock();
    return reply;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply)
{
    mtx.lock();
    if (arg.term > current_term) {
        role = RaftRole::Follower;
        current_term = arg.term;
        vote_for = -1;
        accepted_followers = 0;
        log_storage->SaveSuperBlock(commands.size() - 1, current_term, vote_for);
        set_rpc_time();
    }
    /* Lab3: Your code here */
    if (reply.voteGranted) {
        accepted_followers++;
        std::cerr << "I am " << my_id << ", I got a vote" << std::endl;
        if (accepted_followers > rpc_clients_map.size() / 2) {
            std::cerr << "I am " << my_id << ", I become a leader" << std::endl;
            next_index.clear();
            match_index.clear();
            next_index.resize(rpc_clients_map.size(), commands.size() + log_storage->snapshot_index);
            match_index.resize(rpc_clients_map.size(), log_storage->snapshot_index);
            role = RaftRole::Leader;
        }
    }
    else {

    }
    mtx.unlock();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::append_entries(RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply
{
    /* Lab3: Your code here */

    std::cout << "check 0" << std::endl;
    mtx.lock();
    std::cout << "check 0.5" << std::endl;
    if (rpc_arg.term > current_term) {
        role = RaftRole::Follower;
        current_term = rpc_arg.term;
        vote_for = -1;
        accepted_followers = 0;
        log_storage->SaveSuperBlock(commands.size() - 1, current_term, vote_for);
    }
    AppendEntriesReply reply;
    reply.conflict = -1;
    reply.term = current_term;
    if (rpc_arg.term < current_term)
    {
        reply.success = false;
        mtx.unlock();
        return reply;
    }
    set_rpc_time();
    if (rpc_arg.entries.size() == 0) {
        reply.success = true;


            // leader has been elected
            role = RaftRole::Follower;
            accepted_followers = 0;
            if (rpc_arg.prevLogIndex == indexs.back() && rpc_arg.prevLogTerm == terms.back()) {
                if (role == RaftRole::Follower && rpc_arg.leaderCommit > commit_index)
                    commit_index = rpc_arg.leaderCommit > indexs.back() ? rpc_arg.leaderCommit : indexs.back();
            }
            std::cerr << "I am " << my_id << ", I am a follower, I received a heartbeat from " << rpc_arg.leaderId << ", now my commit num is " << commit_index << ", current term " << current_term << std::endl << std::flush;

        mtx.unlock();
        return reply;
    }

    std::cout << "check 1" << std::endl;
    if(rpc_arg.prevLogIndex > indexs.back() + log_storage->snapshot_index) {
        reply.success = false;
        reply.conflict = indexs.back() + 1;
        std::cout << "I'm " << my_id << ", I can't agree on the index of " << rpc_arg.prevLogIndex << std::endl;
        mtx.unlock();
        return reply;
    }

 //   std::cout << "check 2" << std::endl;
    if (rpc_arg.prevLogTerm != terms[rpc_arg.prevLogIndex - log_storage->snapshot_index]) {
        std::cout << "I'm " << my_id << ", I can't agree on the term of " << rpc_arg.prevLogTerm << std::endl;
        reply.success = false;
        int this_index = rpc_arg.prevLogIndex;
 //       int this_term = rpc_arg.prevLogTerm;
        reply.conflict = this_index + 1;
        mtx.unlock();
        return reply;
    }

//    std::cout << "check 3" << std::endl;
    auto args = transform_rpc_append_entries_args<Command>(rpc_arg);
    for (int i = rpc_arg.prevLogIndex + 1; i < (int)commands.size();) {
        commands.pop_back();
        terms.pop_back();
        indexs.pop_back();
        std::cout << my_id << " popped at " << commands.size() - 1 << std::endl;
    }
    for (int i = 0; i < args.entries.size(); i++) {
        commands.push_back(args.entries[i]);
        terms.push_back(args.terms[i]);
        indexs.push_back(args.indexs[i]);
        log_storage->SaveCommand(args.entries[i], args.terms[i], args.indexs[i]);
        //log_storage->SaveCommand(args.entries[i], args.terms[i], args.indexs[i]);
        std::cout << my_id << "pushed command " << args.entries[i].value << " at term " << args.terms[i] << " index " << commands.size() - 1 << std::endl;
    }

//    std::cout << "check 4" << std::endl;
    if (role == RaftRole::Follower && args.leaderCommit > commit_index)
        commit_index = args.leaderCommit > indexs.back() ? args.leaderCommit : indexs.back();

    reply.success = true;
    mtx.unlock();
    return reply;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_append_entries_reply(int node_id, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply)
{
    mtx.lock();
    std::vector<int> tmp;
    //std::cout << "handling reply" << std::endl;
    if (role != RaftRole::Leader) goto err;
    if (arg.entries.size() == 0) goto err;

    if (!reply.success) {
 //       std::cout << "fail commit" << std::endl;
        if (reply.term > current_term) {
            role = RaftRole::Follower;
            current_term = arg.term;
            vote_for = -1;
            accepted_followers = 0;
            log_storage->SaveSuperBlock(commands.size() - 1, current_term, vote_for);
        }
        else {
            if (next_index[node_id] > 1)
                next_index[node_id]--;

        }
        goto err;
    }

    match_index[node_id] = arg.prevLogIndex + arg.entries.size();
    next_index[node_id] = match_index[node_id] + 1;
    tmp.assign(match_index.begin(), match_index.end());
    sort(tmp.begin(), tmp.end());
//    std::cout << "change commit index from " << commit_index << std::endl;
    commit_index = tmp[tmp.size() / 2 + 1];
//    std::cout << "change commit index to " << commit_index << std::endl;
    mtx.unlock();
    return;

    /* Lab3: Your code here */
    err:
    mtx.unlock();
    return;
}


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args) -> InstallSnapshotReply
{
    mtx.lock();
    InstallSnapshotReply reply;
    if (args.term > current_term) {
        role = RaftRole::Follower;
        current_term = args.term;
        vote_for = -1;
        accepted_followers = 0;
        log_storage->SaveSuperBlock(commands.size() - 1, current_term, vote_for);
    }
    reply.term = current_term;
    if (args.term < current_term) {
        mtx.unlock();
        return reply;
    }
    log_storage->installed_snapshot = 1;
    int command_size = commands.size();
    for(int i = log_storage->snapshot_index; i < (args.lastIncludedIndex < command_size - 1 ? args.lastIncludedIndex : command_size - 1) ; i++) {
        commands.erase(commands.begin());
        terms.erase(terms.begin());
        indexs.erase(indexs.begin());
    }
    commands[0] = Command(0);
    terms[0] = args.lastIncludedTerm;
    indexs[0] = args.lastIncludedIndex;
    log_storage->SaveSnapshot(args.data, args.lastIncludedIndex, args.lastIncludedTerm);
    last_applied = args.lastIncludedIndex;
    commit_index = args.lastIncludedIndex;
    std::cout << "I'm " << my_id << ", args.lastIncludedIndex:" << args.lastIncludedIndex << std::endl;
    log_storage->SaveSuperBlock(commands.size() - 1, current_term, vote_for);
    state->apply_snapshot(args.data);
    /* Lab3: Your code here */
    mtx.unlock();
    return reply;
}


template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(int node_id, const InstallSnapshotArgs arg, const InstallSnapshotReply reply)
{
    /* Lab3: Your code here */
    mtx.lock();
    std::vector<int> tmp;
    if (reply.term > current_term) {
        role = RaftRole::Follower;
        current_term = reply.term;
        vote_for = -1;
        accepted_followers = 0;
        log_storage->SaveSuperBlock(commands.size() - 1, current_term, vote_for);
    }
    next_index[node_id] = log_storage->snapshot_index + 1;
    match_index[node_id] = log_storage->snapshot_index;
    std::cout << "I'm leader " << my_id << ", snapshot: "<< log_storage->snapshot_index << std::endl;
    tmp.assign(match_index.begin(), match_index.end());
    sort(tmp.begin(), tmp.end());
//    std::cout << "change commit index from " << commit_index << std::endl;
    commit_index = tmp[tmp.size() / 2 + 1];
    mtx.unlock();
    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_request_vote(int target_id, RequestVoteArgs arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
//        std::cout << "send request vote fail" << std::endl << std::flush;
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_REQUEST_VOTE, arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_request_vote_reply(target_id, arg, res.unwrap()->as<RequestVoteReply>());
    } else {
        // RPC fails
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_append_entries(int target_id, AppendEntriesArgs<Command> arg)
{
 //   std::cerr << "send " << target_id << "heartbeat" << std::endl << std::flush;
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

 //   std::cerr << "send " << target_id << "heartbeat" << "2" << std::endl << std::flush;
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
 //       std::cerr << "send " << target_id << "heartbeat fail" << std::endl << std::flush;
        return;
    }
 //   std::cerr << "send " << target_id << "heartbeat" << "3" << std::endl << std::flush;

    RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
  //  std::cerr << "send " << target_id << "heartbeat" << "4" << std::endl << std::flush;
    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
  //  std::cerr << "send " << target_id << "heartbeat" << "5" << std::endl << std::flush;
    clients_lock.unlock();
  //  std::cerr << "send " << target_id << "heartbeat" << "6" << std::endl << std::flush;
    if (res.is_ok()) {
  //      std::cerr << "send " << target_id << "heartbeat" << "7" << std::endl << std::flush;
        handle_append_entries_reply(target_id, arg, res.unwrap()->as<AppendEntriesReply>());
   //     std::cerr << "send " << target_id << "heartbeat" << "8" << std::endl << std::flush;
    } else {
        // RPC fails
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_install_snapshot(int target_id, InstallSnapshotArgs arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
    clients_lock.unlock();
    if (res.is_ok()) { 
        handle_install_snapshot_reply(target_id, arg, res.unwrap()->as<InstallSnapshotReply>());
    } else {
        // RPC fails
    }
}

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::send_heartbeat()
    {
        if (role != RaftRole::Leader) return;
//        std::cerr << "I am " << my_id  << ", I am leader, I send my heartbeat" << std::endl;
        AppendEntriesArgs<Command> args;
        args.term = current_term;
        args.leaderId = my_id;
        args.prevLogIndex = indexs.back();
        args.prevLogTerm = terms.back();
        args.leaderCommit = commit_index;
        for (int i = 0; i < (int)rpc_clients_map.size(); i++) {
            if (i == my_id) {
//                std::cerr << "it's myself, no need of heartbeat" << std::endl;
                continue;
            }
            if (rpc_clients_map[i].get() == nullptr) continue;
//            std::cerr << "I am " << my_id  << ", I am leader, I send my heartbeat to "  << i << "," << rpc_clients_map.size() << "in total" << std::endl << std::flush;
            thread_pool->enqueue(&RaftNode<StateMachine, Command>::send_append_entries, this, i, args);
//            std::cerr << i << " " << (int)rpc_clients_map.size() << std::endl << std::flush;

        }
    }

    template <typename StateMachine, typename Command>
    int RaftNode<StateMachine, Command>::random_timeout(){
        int randomSeed = rand() + rand() % 500;
        srand(randomSeed);
        return  300 + (rand() % 400);
        //from internet
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::set_rpc_time(){
            timeval current;
            gettimeofday(&current, NULL);
            last_rpc_time = current.tv_sec*1000 + current.tv_usec/1000;
    }
/******************************************************************

                        Background Workers

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_election() {
    // Periodly check the liveness of the leader.

    // Work for followers and candidates.

    /* Uncomment following code when you finish */
     while (true) {
         {
             if (rpc_clients_map.empty()) continue;
             if (is_stopped()) {

                 return;
             }
             /* Lab3: Your code here */
            timeval now_;
            gettimeofday(&now_, NULL);
            long long now_time = now_.tv_sec * 1000 + now_.tv_usec / 1000;
            int timeout = random_timeout();
            //std::cerr << now_time << " " << last_rpc_time << " " << timeout << std::flush;
            if (rpc_clients_map[my_id].get() == nullptr) {
//                current_term = 0;
                continue;
            }
            int num_of_av = 0;
             for (int i = 0; i < (int)rpc_clients_map.size(); i++) {
                 if (rpc_clients_map[i].get() != nullptr) num_of_av++;

             }
            if (role != RaftRole::Leader && now_time - last_rpc_time > timeout && num_of_av > rpc_clients_map.size() / 2) {
                mtx.lock();
                role = RaftRole::Candidate;
                current_term++;
//                vote_for = my_id;
                log_storage->SaveSuperBlock(commands.size() - 1, current_term, vote_for);
                mtx.unlock();
                last_rpc_time = now_time;
                accepted_followers = 1;
                RequestVoteArgs arg;
                arg.candidate_id = my_id;
                arg.lastLogTerm = terms.back();
                arg.lastLogIndex = indexs.back();
                arg.term = current_term;
                std::cerr << "I want to be a leader, I am " << my_id << std::endl << std::flush;
                for (int i = 0; i < (int)rpc_clients_map.size(); i++) {
                    if (i == my_id) {
                        //std::cout << "oops, its myself" << std::endl << std::flush;
                        continue;
                    }
                    if (rpc_clients_map[i].get() == nullptr) continue;
                    if (role == RaftRole::Follower || role == RaftRole::Leader) {
                        //std::cerr << "I'm a follower now..." << std::endl << std::flush;
                        break;
                    }
                    std::cerr << "I'm " << my_id << " I'm sending to " << i << std::endl << std::flush;
                    thread_pool->enqueue(&RaftNode<StateMachine, Command>::send_request_vote, this, i, arg);
                }

            }
             std::this_thread::sleep_for(std::chrono::milliseconds(150));

         }
     }
    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_commit() {
    // Periodly send logs to the follower.

    // Only work for the leader.

    /* Uncomment following code when you finish */
     while (true) {
         {
             if (rpc_clients_map.empty()) continue;
             if (is_stopped()) {
                 return;
             }
             /* Lab3: Your code here */
             if (role != RaftRole::Leader) continue;
             for (int j = 0; j < rpc_clients_map.size(); j++) {
                 if (rpc_clients_map[my_id].get() == nullptr) break;
                 if (j == my_id) {
 //                    std::cout << "oops, its myself" << std::endl;
                     continue;
                 }
 //                std::cout << "commands size " << commands.size() - 1 << " next index of " << j << ":" << next_index[j] << std::endl;
                 if (rpc_clients_map[j].get() == nullptr) {
 //                    std::cout << "node " << j << " is not working" << std::endl;
                     continue;
                 }
                 std::cout << "I'm " << my_id << "commands size " << commands.size() - 1 << " next index of " << j << ":" << next_index[j] << " snapshot:" << log_storage->snapshot_index << std::endl;

                 if (commands.size() - 1 + log_storage->snapshot_index >= next_index[j] && next_index[j] > log_storage->snapshot_index) {
                     AppendEntriesArgs<Command> args;
                     args.term = current_term;
                     args.leaderId = my_id;
                     args.leaderCommit = commit_index;
                     args.prevLogIndex = next_index[j] - 1;
                     args.prevLogTerm = terms[args.prevLogIndex - log_storage->snapshot_index];
 //                    std::cout << "prevlogind = " << args.prevLogIndex << std::endl;
                     for (int i = args.prevLogIndex + 1 - log_storage->snapshot_index; i < commands.size(); i++) {
                         args.entries.push_back(commands[i]);
                         args.terms.push_back(terms[i]);
                         args.indexs.push_back(indexs[i]);
 //                        std::cout << "leader push back command" << commands[i].value << " at term " << terms[i] << " index " << i << std::endl;
                     }

                     std::cout << "sending to " << j << " commit" << std::endl;
                     thread_pool->enqueue(&RaftNode<StateMachine, Command>::send_append_entries, this, j, args);

//                     std::cout << "successfully commit" << std::endl;

                 }
                 else if (next_index[j] <= log_storage->snapshot_index){
                     std::cout << "sending snapshot to " << j << std::endl;
                     InstallSnapshotArgs args;
                     args.term = current_term;
                     args.data = log_storage->snapshot;
                     args.lastIncludedIndex = log_storage->snapshot_index;
                     args.lastIncludedTerm = log_storage->snapshot_term;
                     std::cout << "args.lastIncludedIndex: " <<  log_storage->snapshot_index << std::endl;
                     thread_pool->enqueue(&RaftNode<StateMachine, Command>::send_install_snapshot, this, j, args);

                     std::this_thread::sleep_for(std::chrono::milliseconds(100));
                 }
             }
             std::this_thread::sleep_for(std::chrono::milliseconds(50));

         }
     }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_apply() {
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

    /* Uncomment following code when you finish */
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:
        // If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
        if (commit_index > last_applied){
            while (last_applied < commit_index) {
                std::cout << "I'm " << my_id << "last_applied: " << last_applied + 1 << " commit_index: " << commit_index << std::endl;
                if (commands.size() - 1 + log_storage->snapshot_index >= commit_index)
                    state->apply_log(commands[++last_applied - log_storage->snapshot_index]);
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.

    /* Uncomment following code when you finish */
     while (true) {
         {
             if (rpc_clients_map.empty()) continue;
             if (is_stopped()) {
                 return;
             }
             if(role == RaftRole::Leader) {
                 if (rpc_clients_map[my_id].get() == nullptr) {
                     role = RaftRole::Follower;
                     continue;
                 }
                 send_heartbeat();
             }

             std::this_thread::sleep_for(std::chrono::milliseconds(40)); // Change the timeout here!
             /* Lab3: Your code here */

         }
     }

    return;
}

/******************************************************************

                          Test Functions (must not edit)

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_network(std::map<int, bool> &network_availability)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    /* turn off network */
    if (!network_availability[my_id]) {
        for (auto &&client: rpc_clients_map) {
            if (client.second != nullptr)
                client.second.reset();
        }

        return;
    }

    for (auto node_network: network_availability) {
        int node_id = node_network.first;
        bool node_status = node_network.second;

        if (node_status && rpc_clients_map[node_id] == nullptr) {
            RaftNodeConfig target_config;
            for (auto config: node_configs) {
                if (config.node_id == node_id) 
                    target_config = config;
            }

            rpc_clients_map[node_id] = std::make_unique<RpcClient>(target_config.ip_address, target_config.port, true);
        }

        if (!node_status && rpc_clients_map[node_id] != nullptr) {
            rpc_clients_map[node_id].reset();
        }
    }
    std::cout << "successfully set " << my_id << " network" << std::endl;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_reliable(bool flag)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    for (auto &&client: rpc_clients_map) {
        if (client.second) {
            client.second->set_reliable(flag);
        }
    }
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::get_list_state_log_num()
{
    /* only applied to ListStateMachine*/
    std::unique_lock<std::mutex> lock(mtx);

    return state->num_append_logs;
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::rpc_count()
{
    int sum = 0;
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    for (auto &&client: rpc_clients_map) {
        if (client.second) {
            sum += client.second->count();
        }
    }
    
    return sum;
}

template <typename StateMachine, typename Command>
std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct()
{
    if (is_stopped()) {
        return std::vector<u8>();
    }

    std::unique_lock<std::mutex> lock(mtx);

    return state->snapshot(); 
}

}