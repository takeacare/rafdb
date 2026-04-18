#ifndef WAL_H_
#define WAL_H_

#include <string>
#include <vector>
#include <stdint.h>
#include <queue>
#include <atomic>
#include "base/mutex.h"
#include "base/thread.h"
#include "base/scoped_ptr.h"
#include "base/condition_variable.h"
#include "global.h"

namespace rafdb {

// WAL日志条目类型
enum LogType {
  LOG_TYPE_NORMAL = 0,  // 普通数据日志
  LOG_TYPE_CONFIG = 1,  // 配置变更日志
  LOG_TYPE_NOOP = 2,    // 空日志（leader当选后发送）
};

// WAL日志条目结构
struct LogEntry {
  uint64_t index;       // 日志索引
  uint64_t term;        // 日志所属任期
  LogType type;         // 日志类型
  std::string dbname;   // 数据库名
  std::string key;      // 键
  std::string value;    // 值
  uint32_t crc;         // 校验和
};

// 日志位置信息：记录日志在哪个段文件的哪个偏移位置
struct LogPosition {
  uint64_t segment_id;  // 段文件ID
  uint64_t offset;      // 文件内偏移
  uint64_t term;        // 日志任期（缓存，用于快速查询）
  
  LogPosition() : segment_id(0), offset(0), term(0) {}
  LogPosition(uint64_t sid, uint64_t off, uint64_t t) 
      : segment_id(sid), offset(off), term(t) {}
};

// 异步刷盘的批量日志
struct AsyncFlushBatch {
  std::vector<LogEntry> entries;
  uint64_t max_index;
  AsyncFlushBatch() : max_index(0) {}
};

class WAL {
 public:
  WAL(const std::string& wal_dir, uint64_t segment_size = 128 * 1024 * 1024); // 默认128MB分段
  ~WAL();

  // 初始化WAL，打开现有文件或创建新文件
  bool Init();

  // 启用异步刷盘
  void EnableAsyncFlush(bool enable, uint64_t flush_interval_ms = 100, 
                        uint64_t batch_size = 100);

  // 追加日志条目，返回是否成功
  bool AppendLog(const LogEntry& entry, bool sync = true);

  // 批量追加日志条目
  bool AppendLogBatch(const std::vector<LogEntry>& entries, bool sync = true);

  // 截断日志到指定索引（删除index之后的日志）
  bool TruncateTo(uint64_t index);

  // 从指定索引开始读取日志，调用callback处理每条日志
  bool ReadFrom(uint64_t index, void (*callback)(const LogEntry&));

  // 按索引获取单条日志
  bool GetLogEntry(uint64_t index, LogEntry* entry);

  // 按索引获取日志的term（快速查询，不需要读取整个日志）
  bool GetLogTerm(uint64_t index, uint64_t* term);

  // 强制刷盘
  bool Sync();

  // 等待直到指定索引的日志被刷盘
  bool WaitForFlush(uint64_t index, int timeout_ms = 5000);

  // 启动时恢复日志，返回最后一条日志的index和term
  bool Recovery(uint64_t* last_index, uint64_t* last_term);

  // 获取当前最后一条日志的index和term
  void GetLastLogInfo(uint64_t* index, uint64_t* term);

  // 获取已刷盘的最后一条日志index
  uint64_t GetLastFlushedIndex();

  // 清理已提交的旧日志，保留最近keep_count条
  bool CleanOldLogs(uint64_t commit_index, uint64_t keep_count = 1000);

  // 停止异步刷盘线程
  void StopAsyncFlush();

 private:
  std::string wal_dir_;          // WAL存储目录
  uint64_t segment_size_;        // 单个WAL文件最大大小
  int current_fd_;               // 当前写入的文件描述符
  uint64_t current_segment_id_;  // 当前段ID
  uint64_t current_offset_;      // 当前文件写入偏移
  uint64_t last_index_;          // 最后一条日志的索引
  uint64_t last_term_;           // 最后一条日志的任期
  base::Mutex mutex_;            // 线程安全锁

  // 内存索引：index -> (segment_id, offset, term)
  // 用于快速查找历史日志的位置和term
  base::hash_map<uint64_t, LogPosition> log_index_;

  // 异步刷盘相关
  bool async_flush_enabled_;
  uint64_t flush_interval_ms_;
  uint64_t batch_size_;
  std::atomic<uint64_t> last_flushed_index_;
  std::atomic<bool> async_flush_running_;
  
  std::queue<LogEntry> pending_flush_queue_;
  base::Mutex flush_mutex_;
  base::ConditionVariable flush_cond_;
  
  scoped_ptr<base::Thread> async_flush_thread_;

  // 异步刷盘线程主循环
  void AsyncFlushLoop();

  // 执行刷盘
  bool DoFlush(const std::vector<LogEntry>& entries);

  // 打开指定段ID的文件
  int OpenSegment(uint64_t segment_id, bool write = false);

  // 滚动到新的段文件
  bool RotateSegment();

  // 计算日志条目的CRC32校验和
  uint32_t CalculateCRC(const LogEntry& entry);

  // 从文件中读取一条日志条目（新版本，使用长度前缀）
  bool ReadEntryV2(int fd, LogEntry* entry, uint64_t* offset);

  // 写入一条日志条目到文件（新版本，使用长度前缀）
  bool WriteEntryV2(int fd, const LogEntry& entry, uint64_t* offset);

  // 旧版本的读写（兼容，实际不再使用）
  bool ReadEntry(int fd, LogEntry* entry, uint64_t* offset);
  bool WriteEntry(int fd, const LogEntry* entry, uint64_t* offset);

  // 构建索引：扫描所有段文件，建立index到位置的映射
  bool BuildIndex();
};

} // namespace rafdb

#endif // WAL_H_
