
#include "storage/rafdb/peer.h"
namespace {
const int kHeartBeatInterval = 1000000;//us
}


namespace rafdb {

void Peer::Run() {
  while ( true ) {
    if (GetRunFlag()) {
      int num = 0;
      size_t node_count = accord_->rafdb_->GetNodeListSize();
      for(size_t i = 0; i < node_count; i++) {
        NodeInfo node_info = accord_->rafdb_->GetNodeInfo(i);
        std::string dest_ip = node_info.ip;
        int dest_port = node_info.port;
        Message mess_send;
        mess_send.term_id = accord_->GetTerm();
        mess_send.ip = accord_->rafdb_->ip_;
        mess_send.port = accord_->rafdb_->port_;
        mess_send.message_type= MessageType::HEARTREQ;
        mess_send.leader_id = accord_->leader_id_;
        //accord_->rafdb_->FillDataSyncInfo(mess_send);//data sync
        bool ret = accord_->sendRPC(dest_ip,dest_port,mess_send,"SendHeartBeat");
        if (!ret) {
          VLOG(5) << "send heart beat fail,dest ip is "<<dest_ip<<" dest port is"<<dest_port;
          num++;
        }else {
          VLOG(5) << "send heart beat succ,dest ip is "<<dest_ip<<" dest port is"<<dest_port;
        }
      }
      setDisconnNums(num);
    }
    usleep(kHeartBeatInterval);
  }
}
}
