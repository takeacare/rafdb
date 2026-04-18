
#ifndef STORAGE_RAFDB_GLOBAL_H_
#define STORAGE_RAFDB_GLOBAL_H_

#include <vector>
#include <string>
#include <stdint.h>

typedef struct NodeInfo{
  std::string ip;
  int port;
  NodeInfo()
  {}
  NodeInfo(const std::string ip_p,const int port_p):ip(ip_p),port(port_p) {
  }
}NodeInfo;

typedef struct LKV{
  std::string dbname;
  std::string key;
  std::string value;
}LKV;

typedef struct LKV_SYNC{
  std::string dbname;
  std::string key;
  std::string value;
  NodeInfo node_info;
  LKV_SYNC(const std::string dbname_p,
            const std::string key_p,
            const std::string value_p,
            const std::string ip_p,
            const int port_p):node_info(ip_p,port_p) {
    dbname = dbname_p;
    key = key_p;
    value = value_p;
  }
}LKV_SYNC;

typedef struct BatchEntry {
  std::string dbname;
  std::string key;
  std::string value;
}BatchEntry;

typedef struct PendingBatch {
  uint64_t start_index;
  uint64_t end_index;
  std::vector<uint64_t> indices;
  bool committed;
  PendingBatch() : start_index(0), end_index(0), committed(false) {}
}PendingBatch;

#endif
