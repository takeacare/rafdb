
#include "storage/rafdb/manager.h"
namespace {
  //2ms
  const int kFlushInterval = 2000;
}

namespace rafdb {

  void Manager::Init(RafDb*rafdb_p) {
    sync_=new Sync(rafdb_p);
  }

  void Manager::Run() {
    while ( true ) {
      if (rafdb_->GetLeaderId() > 0 && rafdb_->GetLeaderId() == rafdb_->self_id_) { // is leader
        LKV *tmp = NULL;
        rafdb_->lkv_queue_.Pop(tmp);
        VLOG(6)<<"get data key : "<<tmp->key;
        if (tmp == NULL) {
          continue;
        }
        size_t node_count = rafdb_->GetNodeListSize();
        for(size_t i = 0; i < node_count; i++)
        {
          NodeInfo node_info = rafdb_->GetNodeInfo(i);
          LKV_SYNC *lkv=new LKV_SYNC(tmp->dbname,tmp->key,tmp->value,node_info.ip,node_info.port);
          sync_->push(lkv);
        }
        delete tmp;
      }
      else {
        usleep(kFlushInterval);
      }

    }
  }
}
