
#include "storage/rafdb/manager.h"
namespace {
  //2ms
  const int kFlushInterval = 2000;
}

namespace rafdb {

  void Manager::Init(RafDb*rafdb_p) {
    // 注意：旧的Sync机制已被禁用
    // 数据同步现在由Raft协议的AppendEntries机制处理
    // 这样可以避免双重同步导致的数据不一致问题
    sync_ = NULL;
  }

  void Manager::Run() {
    // 旧的同步机制已被禁用
    // 原因：
    // 1. 旧机制：LSet -> Set(写LevelDB) -> 推送到lkv_queue_ -> Manager异步同步到其他节点
    //    问题：不等待多数确认，可能导致数据不一致
    //
    // 2. 新机制(Raft协议)：LSet -> RaftSet -> 写WAL -> 发送AppendEntries到所有节点
    //    -> 等待多数确认 -> 提交日志 -> 应用到LevelDB
    //    优点：强一致性，等待多数节点确认后才返回成功
    //
    // 两个机制同时存在会导致：
    // - 数据被复制两次
    // - 旧机制可能在Raft提交之前就修改了数据
    // - 可能导致数据不一致
    
    while (true) {
      // Manager现在只作为占位符
      // 如果将来需要其他后台任务，可以在这里添加
      usleep(kFlushInterval * 1000);
    }
  }
}
