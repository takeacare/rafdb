
#ifndef STORAGE_RAFDB_ACCORD_H_

#define STORAGE_RAFDB_ACCORD_H_

#include <string>
#include <vector>
#include <time.h>
#include <unistd.h>

#include "base/hash_tables.h"
#include "base/mutex.h"
#include "base/timer.h"
#include "base/flags.h"
#include "base/thread.h"
#include "base/scoped_ptr.h"
#include "storage/rafdb/rafdb.h"
#include "storage/rafdb/peer.h"
#include "base/thrift.h"
#include "base/logging.h"
#include "storage/rafdb/proto/gen-cpp/RafdbService.h"
#include "storage/rafdb/wal.h"
//#include "ts/proto/gen-cpp/GeneralCrawlDocServlet.h"



namespace rafdb {

class RafDb;
class Peer;
class Accord :public base::Thread  {
  public:
    explicit Accord(RafDb* rafdb_p,std::string ip,int port)
                         :rafdb_(rafdb_p),ip_(ip),port_(port)
    {
      state_ = State::FOLLOWER;
      term_ = 0;
      vote_id_ = 0;
      leader_id_ = 0;
      commit_index_ = 0;
      last_applied_ = 0;
      Init();
    }
    void Init();
    virtual ~Accord() {}
    
    // 公开的日志复制相关方法
    bool appendLogEntry(const LogEntry& entry);
    uint64_t getLastLogIndex();
    uint64_t getLastLogTerm();
    void sendAppendEntriesToAll();
    bool waitForCommit(uint64_t log_index, int timeout_ms);
    
  protected:
    virtual void Run();

  private:
    int quoramSize(); 
    void SetTerm(int64_t term) {
      base::MutexLock lock(&term_mutex_);
      term_ = term;
    }
    int64_t GetTerm() {
      base::MutexLock lock(&term_mutex_);
      return term_;
    }
    void SetVote(int leader_id) {
      base::MutexLock lock(&vote_mutex_);
      vote_id_ = leader_id;
    }
    int GetVote() {
      base::MutexLock lock(&vote_mutex_);
      return vote_id_;
    }
    int get_rand(int start,int end);
    void stepDown(int64_t term);
    void followerLoop();
    void candidateLoop();
    void leaderLoop();
    void sendQuery();
    void handleHeartRep(const Message& message);
    void sendVotes();
    bool handleMessage(Message& message);
    bool handleVoteReq(const Message& message);
    bool handleHeartReq(Message& message);
    bool handleQueryLeaderReq(const Message& message);
    bool handleAppendEntriesReq(Message& message);
    bool handleAppendEntriesRep(const Message& message);
    bool sendRPC(const std::string ip,const int port,const Message& message,const std::string rpc_name);
    
    // 日志复制相关方法
    void sendAppendEntriesToNode(const NodeInfo& node_info);
    void advanceCommitIndex();
    void applyLogEntries();
    bool getLogTerm(uint64_t index, uint64_t* term);
    
    friend class Peer;
    scoped_ptr<Peer> peer_;
    RafDb* rafdb_;
    State::type state_;    
    base::Mutex term_mutex_;
    base::Mutex vote_mutex_;
    int64_t term_;
    int vote_id_;
    std::string ip_;
    int port_;
    int leader_id_;

    // Raft日志复制相关
    uint64_t commit_index_;       // 已提交的日志索引
    uint64_t last_applied_;       // 已应用到状态机的日志索引
    base::hash_map<std::string, uint64_t> next_index_;   // 每个节点下一个要发送的日志索引
    base::hash_map<std::string, uint64_t> match_index_;  // 每个节点已匹配的最高日志索引
    base::Mutex log_mutex_;       // 日志操作锁

    // 日志应用线程
    void applyLogLoop();
    scoped_ptr<base::Thread> apply_thread_;

    DISALLOW_COPY_AND_ASSIGN(Accord);
};
}





#endif
