#pragma once

#include "rsm/raft/log.h"
#include "rpc/msgpack.hpp"
#include "rsm/list_state_machine.h"

namespace chfs {

const std::string RAFT_RPC_START_NODE = "start node";
const std::string RAFT_RPC_STOP_NODE = "stop node";
const std::string RAFT_RPC_NEW_COMMEND = "new commend";
const std::string RAFT_RPC_CHECK_LEADER = "check leader";
const std::string RAFT_RPC_IS_STOPPED = "check stopped";
const std::string RAFT_RPC_SAVE_SNAPSHOT = "save snapshot";
const std::string RAFT_RPC_GET_SNAPSHOT = "get snapshot";

const std::string RAFT_RPC_REQUEST_VOTE = "request vote";
const std::string RAFT_RPC_APPEND_ENTRY = "append entries";
const std::string RAFT_RPC_INSTALL_SNAPSHOT = "install snapshot";

struct RequestVoteArgs {
    /* Lab3: Your code here */

    int term;
    int candidate_id;
    int lastLogIndex;
    int lastLogTerm;
    MSGPACK_DEFINE(
            term, candidate_id, lastLogIndex, lastLogTerm
    )
};

struct RequestVoteReply {
    /* Lab3: Your code here */
    int term;
    int voteGranted;
    MSGPACK_DEFINE(
        term, voteGranted
    )
};

template <typename Command>
struct AppendEntriesArgs {
    /* Lab3: Your code here */
    int term;
    int leaderId;
    int prevLogIndex;
    int prevLogTerm;
    std::vector<Command> entries;
    std::vector<int> terms;
    std::vector<int> indexs;
    int leaderCommit;
};

struct RpcAppendEntriesArgs {
    /* Lab3: Your code here */
    int term;
    int leaderId;
    int prevLogIndex;
    int prevLogTerm;
    std::vector<int> entries;
    std::vector<int> terms;
    std::vector<int> indexs;
    int leaderCommit;
    MSGPACK_DEFINE(
        term, leaderId, prevLogIndex, prevLogTerm, entries, terms, indexs, leaderCommit
    )
};

template <typename Command>
RpcAppendEntriesArgs transform_append_entries_args(const AppendEntriesArgs<Command> &arg)
{
    /* Lab3: Your code here */
    RpcAppendEntriesArgs rpc_arg;
    rpc_arg.term = arg.term;
    rpc_arg.leaderId = arg.leaderId;
    rpc_arg.prevLogIndex = arg.prevLogIndex;
    rpc_arg.prevLogTerm = arg.prevLogTerm;
    rpc_arg.terms = arg.terms;
    rpc_arg.indexs = arg.indexs;
    rpc_arg.leaderCommit = arg.leaderCommit;
    for (auto entry: arg.entries)
    {
        rpc_arg.entries.push_back(entry.value);
    }
    return rpc_arg;
}

template <typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg)
{
    /* Lab3: Your code here */
    AppendEntriesArgs<Command> arg;
    arg.term = rpc_arg.term;
    arg.leaderId = rpc_arg.leaderId;
    arg.prevLogIndex = rpc_arg.prevLogIndex;
    arg.prevLogTerm = rpc_arg.prevLogTerm;
    arg.terms = rpc_arg.terms;
    arg.indexs = rpc_arg.indexs;
    arg.leaderCommit = rpc_arg.leaderCommit;
    for (auto entry: rpc_arg.entries)
    {
        arg.entries.push_back(Command(entry));
    }
    return arg;
}

struct AppendEntriesReply {
    /* Lab3: Your code here */
    int term;
    bool success = false;
    int conflict;
    MSGPACK_DEFINE(
        term, success, conflict
    )
};

struct InstallSnapshotArgs {
    /* Lab3: Your code here */
    int term;
    int leaderId;
    int lastIncludedIndex;
    int lastIncludedTerm;
    std::vector<u8> data;
    MSGPACK_DEFINE(
        term, leaderId, lastIncludedIndex, lastIncludedTerm, data
    )
};

struct InstallSnapshotReply {
    /* Lab3: Your code here */
    int term;
    MSGPACK_DEFINE(
        term
    )
};

} /* namespace chfs */