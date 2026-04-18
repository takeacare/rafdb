namespace cpp rafdb

enum State {
  FOLLOWER = 0,
  CANDIDATE = 1,
  LEADER = 2,
}

enum LogType {
  LOG_TYPE_NORMAL = 0,
  LOG_TYPE_CONFIG = 1,
  LOG_TYPE_NOOP = 2,
}

struct LogEntry {
  1: i64 index,
  2: i64 term,
  3: LogType type,
  4: string dbname,
  5: string key,
  6: string value,
  7: i32 crc,
}

enum MessageType {
  UNKNOWN = 0,
  VOTEREQ = 1,
  VOTEREP = 2,
  HEARTREQ = 3,
  HEARTREP = 4,
  LEADERREQ = 5,
  LEADERREP = 6,
  APPENDENTRIESREQ = 7,
  APPENDENTRIESREP = 8,
  TIMER = 9,
}

struct Message {
  1:  i64 term_id,
  2:  i32 candidate_id,
  3:  i32 server_id,
  4:  i32 leader_id,
  5:  string ip,
  6:  i32 port,
  7:  bool self_healthy,
  8:  bool granted,
  9:  MessageType message_type = MessageType.UNKNOWN,
  10: bool success,
  // AppendEntries RPC字段
  11: i64 prev_log_index,
  12: i64 prev_log_term,
  13: list<LogEntry> entries,
  14: i64 leader_commit,
  15: i64 match_index,
}



