
#ifndef STORAGE_RAFDB_ACCORD_H_

#define STORAGE_RAFDB_ACCORD_H_

#include <string>
#include <vector>
#include <queue>
#include <map>
#include <time.h>
#include <unistd.h>
#include <stdint.h>
#include <atomic>

#include "base/hash_tables.h"
#include "base/mutex.h"
#include "base/timer.h"
#include "base/flags.h"
#include "base/thread.h"
#include "base/scoped_ptr.h"
#include "base/condition_variable.h"
#include "storage/rafdb/rafdb.h"
#include "storage/rafdb/peer.h"
#include "base/thrift.h"
#include "base/logging.h"
#include "storage/rafdb/proto/gen-cpp/RafdbService.h"
#include "storage/rafdb/wal.h"
#include "storage/rafdb/global.h"

namespace rafdb {

class RafDb;
class Peer;

// Pipeline复制的节点状态
struct PipelineNodeState {
  uint64_t next_inflight_index;  // 下一个待发送的日志索引
  uint64_t highest_inflight;      // 最高的在途日志索引
  uint64_t last_acked_index;      // 最后一个被确认的索引
  bool inflight_available;        // 是否可以发送新的在途请求
  
  PipelineNodeState() 
      : next_inflight_index(0),
        highest_inflight(0),
        last_acked_index(0),
        inflight_available(true) {}
};

// 待提交的批量请求
struct PendingCommit {
  uint64_t start_index;
  uint64_t end_index;
  int64_t submit_time;
  std::vector<uint64_t> indices;
  bool waiting;
  
  PendingCommit() 
      : start_index(0), end_index(0), submit_time(0), waiting(false) {}
};

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
      batch_enabled_ = false;
      pipeline_enabled_ = false;
      async_flush_enabled_ = false;
      batch_size_ = 100;
      batch_timeout_ms_ = 10;
      max_inflight_per_node_ = 10;
      Init();
    }
    void Init();
    virtual ~Accord() {}
    
    // 启用批量提交
    void EnableBatchCommit(bool enable, uint64_t batch_size = 100, 
                           uint64_t batch_timeout_ms = 10);
    
    // 启用Pipeline复制
    void EnablePipeline(bool enable, uint64_t max_inflight = 10);
    
    // 启用异步刷盘（需要配合WAL的异步刷盘）
    void EnableAsyncFlush(bool enable);
    
    // 公开的日志复制相关方法
    bool appendLogEntry(const LogEntry& entry);
    bool appendLogEntries(const std::vector<LogEntry>& entries);
    uint64_t getLastLogIndex();
    uint64_t getLastLogTerm();
    void sendAppendEntriesToAll();
    bool waitForCommit(uint64_t log_index, int timeout_ms);
    bool waitForBatchCommit(const std::vector<uint64_t>& indices, int timeout_ms);
    
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
    
    // 批量提交相关方法
    void batchCommitLoop();
    void flushPendingBatch();
    scoped_ptr<base::Thread> batch_commit_thread_;
    
    // Pipeline复制相关方法
    void sendPipelineAppendEntries();
    void initPipelineState();
    void updatePipelineState(const std::string& node_key, uint64_t acked_index);
    bool canSendInflight(const std::string& node_key);
    
    // 快照相关方法
    bool takeSnapshot(uint64_t* snapshot_index, uint64_t* snapshot_term);
    bool installSnapshot(const Message& message);
    void sendSnapshotToNode(const NodeInfo& node_info);
    bool handleInstallSnapshotReq(Message& message);
    bool handleInstallSnapshotRep(const Message& message);
    
    // 日志压缩相关
    void compactLogs();
    
    // 通知等待提交的请求
    void notifyCommitWaiters(uint64_t commit_index);
    
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

    // 批量提交相关
    bool batch_enabled_;
    uint64_t batch_size_;
    uint64_t batch_timeout_ms_;
    std::queue<LogEntry> pending_batch_;
    base::Mutex batch_mutex_;
    base::ConditionVariable batch_cond_;
    std::atomic<bool> batch_thread_running_;
    
    // Pipeline复制相关
    bool pipeline_enabled_;
    uint64_t max_inflight_per_node_;
    base::hash_map<std::string, PipelineNodeState> pipeline_state_;
    
    // 异步刷盘相关
    bool async_flush_enabled_;
    
    // 提交等待相关
    base::hash_map<uint64_t, base::ConditionVariable*> commit_waiters_;
    base::Mutex commit_wait_mutex_;

    DISALLOW_COPY_AND_ASSIGN(Accord);
};
}

#endif
