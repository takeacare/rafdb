
#include "storage/rafdb/accord.h"
namespace {
  //const int kFlushInterval = 5;
  static const int kElectionTimeout = 3000;// ms
}

namespace rafdb {
void Accord::Run() {
  while ( true ) {
    switch (state_) {
      case State::FOLLOWER:
        VLOG(5) << "now state: FOLLOWER";
        followerLoop();
        break;
      case State::CANDIDATE:
        VLOG(5) << "now state: CANDIDATE";
        candidateLoop();
        break;
      case State::LEADER:
        VLOG(5) << "now state: LEADER";
        leaderLoop();
        break;
      default:
        VLOG(5) << "Unknow state " << state_;
        break;
    }   
  }
}

void Accord::followerLoop() {
  int timeout = get_rand(kElectionTimeout, 2 * kElectionTimeout);// per ms
  VLOG(5) << "follower timeout is "<<timeout;
  base::Timer timer;
  timer.After(timeout);
  bool update;
  while (true) {
    update = false;
    if (timer.IsReadable()) {
      VLOG(5) << "follower timeout!!!, switch to candidate.";
      state_ = State::CANDIDATE;
      break;
    }
    Message tmp_message;
    if(rafdb_->message_queue_.TryPop(tmp_message)) {
      update = handleMessage(tmp_message);
    }
    if(update) {
      timer.After(get_rand(kElectionTimeout, 2 * kElectionTimeout));
    }
    usleep(20);//sleep 20us
  }
}

void Accord::candidateLoop() {
  int timeout = get_rand(kElectionTimeout, 2 * kElectionTimeout);// per ms
  VLOG(5) << "candidate timeout is "<<timeout;
  base::Timer timer;
  leader_id_ = 0;
  rafdb_->SetLeaderId(0);
  VLOG(5) << "candidate set  leader_id 0";
  int grants = 1;
  int noleader = 1;
  timer.After(timeout);
  VLOG(5) << "start query leader";
  sendQuery();
  while (true) {
    if (timer.IsReadable()) {
      VLOG(5) << "candidate timeout!!!";
      break;
    } 
    Message tmp_message;
    if (rafdb_->message_queue_.TryPop(tmp_message)) {
      if (tmp_message.message_type == MessageType::LEADERREP) {
        VLOG(5)<<"reveive message,type is LEADERREP";
        if (tmp_message.leader_id == 0) {
          VLOG(5)<<"leader_id is 0,noleader++";
          noleader++;
        }
        if (noleader >= quoramSize()) {
          VLOG(5) << "noleader gt half,start vote";
          SetTerm(GetTerm() + 1);
          SetVote(rafdb_->self_id_);
          VLOG(5)<<"start send vote";
          sendVotes();
        }
      }else if (tmp_message.message_type == MessageType::VOTEREP) {
        VLOG(5)<<"reveive message,type is VOTEREP";
        VLOG(5)<<"message.term_id is"<<tmp_message.term_id<<
          " current term is "<<GetTerm();
        if (tmp_message.term_id > GetTerm()) {
          VLOG(5)<<"message.term_id is"<<tmp_message.term_id
            <<" current term is "<<GetTerm()<<" stepDown to follower";
          stepDown(tmp_message.term_id);
        }
        if (tmp_message.granted) {
          VLOG(5) << "receive grant";
          grants++;
        }
        
      }else {
        handleMessage(tmp_message);
      }
    }
    if (grants >= quoramSize()) {
      VLOG(5) <<"grants is"<<grants<<"quoramSize is "<<quoramSize()<< " gt half agree,step leader";
      state_ = State::LEADER;
      break;
    }
    
    if (state_ != State::CANDIDATE) {
      break;
    }
    usleep(20);
  }
}

struct SendQueryContext {
  Accord* accord;
  Message* mess_send;
};

void SendQueryCallback(const NodeInfo& node_info) {
  // 这个回调需要上下文，我们改用另一种方式
}

void Accord::sendQuery() {
  size_t node_count = rafdb_->GetNodeListSize();
  for (size_t i = 0; i < node_count; i++) {
    NodeInfo node_info = rafdb_->GetNodeInfo(i);
    std::string dest_ip = node_info.ip;
    int dest_port = node_info.port;
    VLOG(5) << "sendQuery" << "ip is "<<dest_ip<<" port is "<<dest_port;
    Message mess_send;
    mess_send.ip = rafdb_->ip_;
    mess_send.port = rafdb_->port_;
    mess_send.message_type= MessageType::LEADERREQ;
    sendRPC(dest_ip,dest_port,mess_send,"QueryLeaderId");
  }
}

void Accord::sendVotes() {
  Message mess_send;
  mess_send.ip = rafdb_->ip_;
  mess_send.port = rafdb_->port_;
  mess_send.message_type= MessageType::VOTEREQ;
  mess_send.term_id = GetTerm();
  mess_send.self_healthy = rafdb_->SelfHealthy();
  mess_send.candidate_id = rafdb_->self_id_;
  size_t node_count = rafdb_->GetNodeListSize();
  for (size_t i = 0; i < node_count; i++) {
    NodeInfo node_info = rafdb_->GetNodeInfo(i);
    std::string dest_ip = node_info.ip;
    int dest_port = node_info.port;
    VLOG(5) << "sendVotes" << "ip is "<<dest_ip<<" port is "<<dest_port
      <<" term is "<<mess_send.term_id;
    sendRPC(dest_ip,dest_port,mess_send,"SendVote");
  }
}

void Accord::leaderLoop() {
  leader_id_ = rafdb_->self_id_;
  rafdb_->SetLeaderId(leader_id_);
  peer_->SetRunFlag(true);
  
  // 初始化next_index和match_index
  {
    base::MutexLock lock(&log_mutex_);
    uint64_t last_log_index = getLastLogIndex();
    size_t node_count = rafdb_->GetNodeListSize();
    for (size_t i = 0; i < node_count; i++) {
      NodeInfo node_info = rafdb_->GetNodeInfo(i);
      std::string node_key = node_info.ip + ":" + std::to_string(node_info.port);
      next_index_[node_key] = last_log_index + 1;
      match_index_[node_key] = 0;
    }
  }
  
  // 发送一个空的AppendEntries作为心跳
  sendAppendEntriesToAll();
  
  const int kHeartbeatIntervalMs = 100; // 100ms心跳间隔
  const int kApplyLogIntervalMs = 10; // 10ms检查一次日志应用
  int heartbeat_counter = 0;
  
  while (true) {
    if (rafdb_->SelfHealthy() == false) {
      VLOG(5) << "leader is not healthy,switch to follower";
      state_ = State::FOLLOWER;//suicide
    }
    if (peer_->getDisconnNums() >= quoramSize()) {
      VLOG(5)<< "followers disconnect num gt half,switch follower";
      state_ = State::FOLLOWER;
    }
    Message tmp_message; 
    if (rafdb_->message_queue_.TryPop(tmp_message)) {
      handleMessage(tmp_message);
    }
    
    // 定期发送心跳（AppendEntries）
    heartbeat_counter++;
    if (heartbeat_counter >= kHeartbeatIntervalMs / kApplyLogIntervalMs) {
      heartbeat_counter = 0;
      sendAppendEntriesToAll();
    }
    
    // 应用已提交的日志到状态机
    applyLogEntries();
    
    if (state_ != State::LEADER) {
      peer_->SetRunFlag(false);
      break;
    }
    usleep(kApplyLogIntervalMs * 1000);
  }

}

void Accord::Init() {
  peer_.reset(new Peer(this));
  peer_->Start();//heart beat thread
}

bool Accord::handleMessage(Message& message) {
  if (message.message_type == MessageType::VOTEREQ) {
    return handleVoteReq(message);//process vote req
  }else if (message.message_type == MessageType::HEARTREQ) {
    return handleHeartReq(message);// process heartbeat
  }else if (message.message_type == MessageType::LEADERREQ) {
    return handleQueryLeaderReq(message);
  }else if (message.message_type == MessageType::APPENDENTRIESREQ) {
    return handleAppendEntriesReq(message);
  }else if (message.message_type == MessageType::APPENDENTRIESREP) {
    return handleAppendEntriesRep(message);
  }
  return false;
}

bool Accord::handleHeartReq(Message& message) {
  VLOG(5) << "receive hear req "<<
    "current term is"<<GetTerm()<<" leader term is"<<message.term_id;
  int64_t currentTerm = GetTerm();
  int serverId = rafdb_->self_id_;
  std::string dest_ip = message.ip;
  int dest_port = message.port;
  if (message.term_id < currentTerm) {
    // from old leader
    Message mess_send;
    mess_send.term_id = currentTerm;
    mess_send.server_id = serverId;
    mess_send.ip = rafdb_->ip_;
    mess_send.port = rafdb_->port_;
    mess_send.message_type= MessageType::HEARTREP;
    mess_send.success = false;
    sendRPC(dest_ip,dest_port,mess_send,"ReplyHeartBeat");
    return false;
  }  
 
  if (message.term_id >= currentTerm) {
    stepDown(message.term_id);
  }

  rafdb_->SetLeaderId(message.leader_id);
  //rafdb_->DataSyncLocal(message);
  //VLOG(5)<<"data sync,host_inst_count_map size is"<<
  //  message.data_sync.host_inst_count_map.size()<<
  //  "host_normal_count_map size is"<<message.data_sync.host_normal_count_map.size()<<
  //  "last_handler_index is " <<message.data_sync.last_handler_index<<
  //  "last_receiver_index is "<<message.data_sync.last_receiver_index;
  leader_id_ = message.leader_id;
  Message mess_send;
  mess_send.term_id = currentTerm;
  mess_send.server_id = serverId;
  mess_send.ip = rafdb_->ip_;
  mess_send.port = rafdb_->port_;
  mess_send.message_type= MessageType::HEARTREP;
  mess_send.success = true;
  sendRPC(dest_ip,dest_port,mess_send,"ReplyHeartBeat");
  return true;

}

bool Accord::handleQueryLeaderReq(const Message& message) {
  int64_t currentTerm = GetTerm();
  if (state_ == State::CANDIDATE) {
    std::string dest_ip = message.ip;
    int dest_port = message.port;
    Message mess_send;
    mess_send.term_id = currentTerm;
    mess_send.ip = rafdb_->ip_;
    mess_send.port = rafdb_->port_;
    mess_send.message_type= MessageType::LEADERREP;
    mess_send.leader_id = 0;
    sendRPC(dest_ip,dest_port,mess_send,"ReplyLeaderId");
  }else {
    std::string dest_ip = message.ip;
    int dest_port = message.port;
    Message mess_send;
    mess_send.term_id = currentTerm;
    mess_send.ip = rafdb_->ip_;
    mess_send.port = rafdb_->port_;
    mess_send.message_type= MessageType::LEADERREP;
    mess_send.leader_id = leader_id_;
    sendRPC(dest_ip,dest_port,mess_send,"ReplyLeaderId");
  }
  return false;
}

void Accord::stepDown(int64_t term) {
  if (term > GetTerm()) {
    SetTerm(term);
    SetVote(0);
  }
  state_ = State::FOLLOWER;
  //rafdb_->SetLeaderId(0);
}

bool Accord::handleVoteReq(const Message& message) {
  int64_t currentTerm = GetTerm();
  std::string dest_ip = message.ip;
  int dest_port = message.port;
  VLOG(5) << "receive vote,self term is "<<currentTerm
      <<" voter term is "<<message.term_id;
  if (message.term_id < currentTerm) {
    VLOG(5)<<"refuse";
    //VLOG(5) << ""
    Message mess_send;
    mess_send.term_id = currentTerm;
    mess_send.ip = rafdb_->ip_;
    mess_send.port = rafdb_->port_;
    mess_send.self_healthy = rafdb_->SelfHealthy();
    mess_send.granted = false;
    mess_send.message_type= MessageType::VOTEREP;
    sendRPC(dest_ip,dest_port,mess_send,"ReplyVote");
    return false;
  }
  if (message.term_id > currentTerm) {
    VLOG(5)<<"step down";
    stepDown(message.term_id);
  }
  if (vote_id_ != 0 && vote_id_ != message.candidate_id) {
    VLOG(5)<<"had voted,but not this man,so refuse,because everyone have only one vote chance";
    VLOG(5)<<"vote_id is "<<vote_id_<<" message.candidate_id is "<<message.candidate_id;
    Message mess_send;
    mess_send.term_id = currentTerm;
    mess_send.ip = rafdb_->ip_;
    mess_send.port = rafdb_->port_;
    mess_send.granted = false;
    mess_send.message_type= MessageType::VOTEREP;
    sendRPC(dest_ip,dest_port,mess_send,"ReplyVote");
    return false;
  }
  if (message.self_healthy) {
    VLOG(5)<<"he is healthy,i agree";
    SetVote(message.candidate_id);
    Message mess_send;
    mess_send.term_id = currentTerm;
    mess_send.ip = rafdb_->ip_;
    mess_send.port = rafdb_->port_;
    mess_send.granted = true;
    mess_send.message_type= MessageType::VOTEREP;
    sendRPC(dest_ip,dest_port,mess_send,"ReplyVote");
    return true;

  } // judge healthy
  VLOG(5) << "term_id gt than myself,but he is not healthy,so refuse";
  Message mess_send;
  mess_send.term_id = currentTerm;
  mess_send.ip = rafdb_->ip_;
  mess_send.port = rafdb_->port_;
  mess_send.granted = false;
  mess_send.message_type= MessageType::VOTEREP;
  sendRPC(dest_ip,dest_port,mess_send,"ReplyVote");
  return false;
}

bool Accord::sendRPC(const std::string ip,const int port,
    const Message& message,const std::string rpc_name) {
  
  base::ThriftClient<RafdbServiceClient> thrift_client(ip,port);
  try {
    if (thrift_client.GetService() == NULL) {
      thrift_client.GetTransport()->close();
      return false;
    }else {
      if (rpc_name == "SendVote") {
        thrift_client.GetService()->SendVote(message);
        thrift_client.GetTransport()->close();
        return true;
      }else if (rpc_name == "ReplyVote") {
        thrift_client.GetService()->ReplyVote(message);
        thrift_client.GetTransport()->close();
        return true;
      }else if (rpc_name == "SendHeartBeat") {
        thrift_client.GetService()->SendHeartBeat(message);
        thrift_client.GetTransport()->close();
        return true;
      }else if (rpc_name == "ReplyHeartBeat") {
        thrift_client.GetService()->ReplyHeartBeat(message);
        thrift_client.GetTransport()->close();
        return true;
      }else if (rpc_name == "QueryLeaderId") {
        thrift_client.GetService()->QueryLeaderId(message);
        thrift_client.GetTransport()->close();
        return true;
      }else if (rpc_name == "ReplyLeaderId") {
        thrift_client.GetService()->ReplyLeaderId(message);
        thrift_client.GetTransport()->close();
        return true;
      }else if (rpc_name == "SendAppendEntries") {
        thrift_client.GetService()->SendAppendEntries(message);
        thrift_client.GetTransport()->close();
        return true;
      }else if (rpc_name == "ReplyAppendEntries") {
        thrift_client.GetService()->ReplyAppendEntries(message);
        thrift_client.GetTransport()->close();
        return true;
      }else {
        thrift_client.GetTransport()->close();
        return false;
      }
    }
  } catch (const TException &tx) {
    thrift_client.GetTransport()->close();
    return false;
  }
}


int Accord::quoramSize() {
  return (rafdb_->GetNodeListSize()+1) / 2 + 1;
}

void Accord::handleHeartRep(const Message& message) {
  if (message.term_id < GetTerm()) {
    return;
  }
  if (message.term_id > GetTerm()) {
    stepDown(message.term_id);
  }
  if (!message.success) {
    return;
  }
}

int Accord::get_rand(int start,int end) {
      if (end <= start)
        return start;
      srand(rafdb_->self_id_);
      return (rand() % (end-start))+ start;
    }

// 处理AppendEntries请求（Follower处理Leader的日志复制请求）
bool Accord::handleAppendEntriesReq(Message& message) {
  VLOG(5) << "receive AppendEntries req, current term: " << GetTerm() 
          << ", leader term: " << message.term_id;
  
  int64_t currentTerm = GetTerm();
  std::string dest_ip = message.ip;
  int dest_port = message.port;
  
  // 1. 如果领导人的任期小于当前任期，返回失败
  if (message.term_id < currentTerm) {
    Message mess_send;
    mess_send.term_id = currentTerm;
    mess_send.server_id = rafdb_->self_id_;
    mess_send.ip = rafdb_->ip_;
    mess_send.port = rafdb_->port_;
    mess_send.message_type = MessageType::APPENDENTRIESREP;
    mess_send.success = false;
    sendRPC(dest_ip, dest_port, mess_send, "ReplyAppendEntries");
    return false;
  }
  
  // 2. 如果领导人任期大于等于当前任期，转换为Follower
  if (message.term_id >= currentTerm) {
    stepDown(message.term_id);
  }
  
  rafdb_->SetLeaderId(message.leader_id);
  leader_id_ = message.leader_id;
  
  base::MutexLock lock(&log_mutex_);
  
  // 3. 如果prevLogIndex处的日志任期与prevLogTerm不匹配，返回失败
  uint64_t prev_log_index = static_cast<uint64_t>(message.prev_log_index);
  uint64_t prev_log_term = static_cast<uint64_t>(message.prev_log_term);
  
  if (prev_log_index > 0) {
    uint64_t log_term = 0;
    if (!getLogTerm(prev_log_index, &log_term) || log_term != prev_log_term) {
      VLOG(5) << "AppendEntries: log mismatch at index " << prev_log_index;
      Message mess_send;
      mess_send.term_id = GetTerm();
      mess_send.server_id = rafdb_->self_id_;
      mess_send.ip = rafdb_->ip_;
      mess_send.port = rafdb_->port_;
      mess_send.message_type = MessageType::APPENDENTRIESREP;
      mess_send.success = false;
      mess_send.match_index = getLastLogIndex();
      sendRPC(dest_ip, dest_port, mess_send, "ReplyAppendEntries");
      return true;
    }
  }
  
  // 4. 追加新的日志条目
  for (size_t i = 0; i < message.entries.size(); i++) {
    const LogEntry& entry = message.entries[i];
    uint64_t entry_index = static_cast<uint64_t>(entry.index);
    
    // 检查是否已有冲突的日志
    uint64_t existing_term = 0;
    if (getLogTerm(entry_index, &existing_term)) {
      if (existing_term != static_cast<uint64_t>(entry.term)) {
        // 冲突，截断日志
        if (rafdb_->wal_) {
          rafdb_->wal_->TruncateTo(entry_index - 1);
        }
        // 重新写入
        if (rafdb_->wal_) {
          LogEntry wal_entry;
          wal_entry.index = entry_index;
          wal_entry.term = static_cast<uint64_t>(entry.term);
          wal_entry.type = static_cast<LogType>(entry.type);
          wal_entry.dbname = entry.dbname;
          wal_entry.key = entry.key;
          wal_entry.value = entry.value;
          rafdb_->wal_->AppendLog(wal_entry);
        }
      }
    } else {
      // 新日志，直接追加
      if (rafdb_->wal_) {
        LogEntry wal_entry;
        wal_entry.index = entry_index;
        wal_entry.term = static_cast<uint64_t>(entry.term);
        wal_entry.type = static_cast<LogType>(entry.type);
        wal_entry.dbname = entry.dbname;
        wal_entry.key = entry.key;
        wal_entry.value = entry.value;
        rafdb_->wal_->AppendLog(wal_entry);
      }
    }
  }
  
  // 5. 如果leaderCommit > commitIndex，更新commitIndex
  uint64_t leader_commit = static_cast<uint64_t>(message.leader_commit);
  if (leader_commit > commit_index_) {
    uint64_t last_log_index = getLastLogIndex();
    commit_index_ = (leader_commit < last_log_index) ? leader_commit : last_log_index;
    VLOG(5) << "AppendEntries: update commit_index to " << commit_index_;
  }
  
  // 回复成功
  Message mess_send;
  mess_send.term_id = GetTerm();
  mess_send.server_id = rafdb_->self_id_;
  mess_send.ip = rafdb_->ip_;
  mess_send.port = rafdb_->port_;
  mess_send.message_type = MessageType::APPENDENTRIESREP;
  mess_send.success = true;
  mess_send.match_index = getLastLogIndex();
  sendRPC(dest_ip, dest_port, mess_send, "ReplyAppendEntries");
  
  return true;
}

// 处理AppendEntries响应（Leader处理Follower的回复）
bool Accord::handleAppendEntriesRep(const Message& message) {
  VLOG(5) << "receive AppendEntries rep, success: " << message.success
          << ", match_index: " << message.match_index;
  
  if (message.term_id < GetTerm()) {
    return false;
  }
  if (message.term_id > GetTerm()) {
    stepDown(message.term_id);
    return false;
  }
  
  std::string node_key = message.ip + ":" + std::to_string(message.port);
  base::MutexLock lock(&log_mutex_);
  
  if (message.success) {
    // 更新match_index和next_index
    uint64_t new_match = static_cast<uint64_t>(message.match_index);
    if (new_match > match_index_[node_key]) {
      match_index_[node_key] = new_match;
      next_index_[node_key] = new_match + 1;
      VLOG(5) << "Update match_index for " << node_key << " to " << new_match;
    }
    // 尝试推进commit_index
    advanceCommitIndex();
  } else {
    // 日志不匹配，回退next_index重试
    if (next_index_[node_key] > 1) {
      next_index_[node_key]--;
      VLOG(5) << "Decrement next_index for " << node_key << " to " << next_index_[node_key];
    }
  }
  
  return true;
}

// 向所有节点发送AppendEntries
void Accord::sendAppendEntriesToAll() {
  if (state_ != State::LEADER) {
    return;
  }
  
  size_t node_count = rafdb_->GetNodeListSize();
  for (size_t i = 0; i < node_count; i++) {
    NodeInfo node_info = rafdb_->GetNodeInfo(i);
    sendAppendEntriesToNode(node_info);
  }
}

// 向单个节点发送AppendEntries
void Accord::sendAppendEntriesToNode(const NodeInfo& node_info) {
  base::MutexLock lock(&log_mutex_);
  
  std::string node_key = node_info.ip + ":" + std::to_string(node_info.port);
  
  // 确保next_index已初始化
  if (next_index_.find(node_key) == next_index_.end()) {
    next_index_[node_key] = getLastLogIndex() + 1;
    match_index_[node_key] = 0;
  }
  
  uint64_t next_idx = next_index_[node_key];
  uint64_t prev_log_index = next_idx - 1;
  uint64_t prev_log_term = 0;
  
  if (prev_log_index > 0) {
    getLogTerm(prev_log_index, &prev_log_term);
  }
  
  // 构建消息
  Message mess_send;
  mess_send.term_id = GetTerm();
  mess_send.leader_id = rafdb_->self_id_;
  mess_send.ip = rafdb_->ip_;
  mess_send.port = rafdb_->port_;
  mess_send.message_type = MessageType::APPENDENTRIESREQ;
  mess_send.prev_log_index = prev_log_index;
  mess_send.prev_log_term = prev_log_term;
  mess_send.leader_commit = commit_index_;
  
  // 收集需要发送的日志条目
  // 简化实现：每次只发送一条日志，或者发送空的心跳
  // 实际实现中可以批量发送
  
  // 发送RPC
  sendRPC(node_info.ip, node_info.port, mess_send, "SendAppendEntries");
}

// 追加日志条目（Leader调用）
bool Accord::appendLogEntry(const LogEntry& entry) {
  if (state_ != State::LEADER) {
    return false;
  }
  
  base::MutexLock lock(&log_mutex_);
  
  uint64_t new_index = getLastLogIndex() + 1;
  LogEntry new_entry = entry;
  new_entry.index = new_index;
  new_entry.term = GetTerm();
  
  // 写入WAL
  if (rafdb_->wal_) {
    if (!rafdb_->wal_->AppendLog(new_entry)) {
      LOG(ERROR) << "Failed to append log entry to WAL";
      return false;
    }
  }
  
  VLOG(5) << "Appended log entry, index: " << new_index 
          << ", term: " << new_entry.term;
  
  return true;
}

// 推进commit_index
void Accord::advanceCommitIndex() {
  // 找到最大的N，使得大多数节点的match_index >= N，且N > commit_index_
  uint64_t last_log_index = getLastLogIndex();
  
  for (uint64_t N = commit_index_ + 1; N <= last_log_index; N++) {
    uint64_t log_term = 0;
    if (!getLogTerm(N, &log_term)) {
      continue;
    }
    
    // 只有当前任期的日志才能被提交
    if (log_term != GetTerm()) {
      continue;
    }
    
    // 统计有多少节点的match_index >= N
    int match_count = 1; // 自己已经匹配
    size_t node_count = rafdb_->GetNodeListSize();
    for (size_t i = 0; i < node_count; i++) {
      NodeInfo node_info = rafdb_->GetNodeInfo(i);
      std::string node_key = node_info.ip + ":" + std::to_string(node_info.port);
      if (match_index_[node_key] >= N) {
        match_count++;
      }
    }
    
    if (match_count >= quoramSize()) {
      commit_index_ = N;
      VLOG(5) << "Advanced commit_index to " << N;
    }
  }
}

// 应用日志条目到状态机
void Accord::applyLogEntries() {
  base::MutexLock lock(&log_mutex_);
  
  while (last_applied_ < commit_index_) {
    uint64_t apply_index = last_applied_ + 1;
    VLOG(5) << "Applying log entry " << apply_index;
    
    // 从WAL读取日志条目
    LogEntry entry;
    if (rafdb_->wal_ && rafdb_->wal_->GetLogEntry(apply_index, &entry)) {
      VLOG(5) << "Read log entry: index=" << entry.index 
              << ", dbname=" << entry.dbname 
              << ", key=" << entry.key;
      
      // 应用到状态机（写入LevelDB）
      if (entry.type == LOG_TYPE_NORMAL) {
        rafdb_->ApplyLogEntry(entry.dbname, entry.key, entry.value);
        VLOG(5) << "Applied log entry " << apply_index << " to LevelDB";
      }
    } else {
      LOG(ERROR) << "Failed to read log entry " << apply_index << " from WAL";
    }
    
    last_applied_ = apply_index;
  }
}

// 获取最后一条日志的索引
uint64_t Accord::getLastLogIndex() {
  if (rafdb_->wal_) {
    uint64_t index = 0, term = 0;
    rafdb_->wal_->GetLastLogInfo(&index, &term);
    return index;
  }
  return 0;
}

// 获取最后一条日志的任期
uint64_t Accord::getLastLogTerm() {
  if (rafdb_->wal_) {
    uint64_t index = 0, term = 0;
    rafdb_->wal_->GetLastLogInfo(&index, &term);
    return term;
  }
  return 0;
}

// 获取指定索引的日志任期
bool Accord::getLogTerm(uint64_t index, uint64_t* term) {
  *term = 0;
  
  // 调用WAL的GetLogTerm方法（快速查询，通过内存索引）
  if (rafdb_->wal_) {
    return rafdb_->wal_->GetLogTerm(index, term);
  }
  
  // 如果没有WAL，只返回最后一条日志的term
  uint64_t last_index = getLastLogIndex();
  uint64_t last_term = getLastLogTerm();
  
  if (index == last_index) {
    *term = last_term;
    return true;
  }
  
  return false;
}

// 等待日志提交
bool Accord::waitForCommit(uint64_t log_index, int timeout_ms) {
  const int sleep_interval = 10; // ms
  int waited = 0;
  
  while (waited < timeout_ms) {
    {
      base::MutexLock lock(&log_mutex_);
      if (commit_index_ >= log_index) {
        return true;
      }
    }
    usleep(sleep_interval * 1000);
    waited += sleep_interval;
  }
  
  return false;
}

}
