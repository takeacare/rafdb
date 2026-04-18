#include "wal.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>
#include <cstring>
#include <algorithm>
#include "base/logging.h"

namespace rafdb {

// 日志文件头魔法数
const uint32_t kMagicNumber = 0x57414C30; // "WAL0"
// 日志条目头部大小（V2版本）：
// magic(4) + total_len(4) + type(1) + index(8) + term(8) + 
// dbname_len(4) + key_len(4) + value_len(4) + crc(4) = 42字节
const uint32_t kEntryHeaderSizeV2 = 42;

WAL::WAL(const std::string& wal_dir, uint64_t segment_size)
    : wal_dir_(wal_dir),
      segment_size_(segment_size),
      current_fd_(-1),
      current_segment_id_(0),
      current_offset_(0),
      last_index_(0),
      last_term_(0) {
}

WAL::~WAL() {
  if (current_fd_ >= 0) {
    Sync();
    close(current_fd_);
    current_fd_ = -1;
  }
}

bool WAL::Init() {
  base::MutexLock lock(&mutex_);
  // 创建目录
  if (mkdir(wal_dir_.c_str(), 0755) != 0 && errno != EEXIST) {
    LOG(ERROR) << "Failed to create wal directory: " << wal_dir_ << ", error: " << strerror(errno);
    return false;
  }

  // 扫描现有段文件
  std::vector<uint64_t> segment_ids;
  DIR* dir = opendir(wal_dir_.c_str());
  if (dir) {
    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {
      if (strstr(entry->d_name, "wal_") == entry->d_name) {
        uint64_t id = atoll(entry->d_name + 4);
        segment_ids.push_back(id);
      }
    }
    closedir(dir);
  }

  // 找到最大的段ID
  if (!segment_ids.empty()) {
    std::sort(segment_ids.begin(), segment_ids.end());
    current_segment_id_ = segment_ids.back();
  } else {
    current_segment_id_ = 1;
  }

  // 打开当前段文件
  current_fd_ = OpenSegment(current_segment_id_, true);
  if (current_fd_ < 0) {
    LOG(ERROR) << "Failed to open current wal segment: " << current_segment_id_;
    return false;
  }

  // 定位到文件末尾
  current_offset_ = lseek(current_fd_, 0, SEEK_END);
  if (current_offset_ < 0) {
    LOG(ERROR) << "Failed to seek to end of wal file: " << strerror(errno);
    return false;
  }

  return true;
}

int WAL::OpenSegment(uint64_t segment_id, bool write) {
  std::string file_path = wal_dir_ + "/wal_" + std::to_string(segment_id);
  int flags = O_RDONLY;
  if (write) {
    flags = O_WRONLY | O_CREAT | O_APPEND;
  }
  int fd = open(file_path.c_str(), flags, 0644);
  if (fd < 0) {
    LOG(ERROR) << "Failed to open wal file " << file_path << ", error: " << strerror(errno);
  }
  return fd;
}

bool WAL::RotateSegment() {
  if (current_fd_ >= 0) {
    Sync();
    close(current_fd_);
  }
  current_segment_id_++;
  current_fd_ = OpenSegment(current_segment_id_, true);
  if (current_fd_ < 0) {
    LOG(ERROR) << "Failed to rotate to new wal segment: " << current_segment_id_;
    return false;
  }
  current_offset_ = 0;
  return true;
}

uint32_t WAL::CalculateCRC(const LogEntry& entry) {
  // 简单CRC32实现，实际可替换为优化版本
  uint32_t crc = 0xFFFFFFFF;
  const uint8_t* data = reinterpret_cast<const uint8_t*>(&entry.index);
  for (size_t i = 0; i < sizeof(entry.index) + sizeof(entry.term) + sizeof(entry.type); i++) {
    crc ^= data[i];
    for (int j = 0; j < 8; j++) {
      crc = (crc >> 1) ^ (0xEDB88320 * (crc & 1));
    }
  }
  for (char c : entry.dbname) {
    crc ^= c;
    for (int j = 0; j < 8; j++) {
      crc = (crc >> 1) ^ (0xEDB88320 * (crc & 1));
    }
  }
  for (char c : entry.key) {
    crc ^= c;
    for (int j = 0; j < 8; j++) {
      crc = (crc >> 1) ^ (0xEDB88320 * (crc & 1));
    }
  }
  for (char c : entry.value) {
    crc ^= c;
    for (int j = 0; j < 8; j++) {
      crc = (crc >> 1) ^ (0xEDB88320 * (crc & 1));
    }
  }
  return crc ^ 0xFFFFFFFF;
}

// V2版本：使用长度前缀，正确的序列化格式
bool WAL::WriteEntryV2(int fd, const LogEntry& entry, uint64_t* offset) {
  uint32_t dbname_len = static_cast<uint32_t>(entry.dbname.size());
  uint32_t key_len = static_cast<uint32_t>(entry.key.size());
  uint32_t value_len = static_cast<uint32_t>(entry.value.size());
  uint32_t total_len = kEntryHeaderSizeV2 + dbname_len + key_len + value_len;

  // 分配缓冲区
  std::vector<uint8_t> buf(total_len);
  uint8_t* ptr = buf.data();

  // 写入头部
  *reinterpret_cast<uint32_t*>(ptr) = kMagicNumber; ptr += 4;
  *reinterpret_cast<uint32_t*>(ptr) = total_len; ptr += 4;
  *ptr = static_cast<uint8_t>(entry.type); ptr += 1;
  *reinterpret_cast<uint64_t*>(ptr) = entry.index; ptr += 8;
  *reinterpret_cast<uint64_t*>(ptr) = entry.term; ptr += 8;
  *reinterpret_cast<uint32_t*>(ptr) = dbname_len; ptr += 4;
  *reinterpret_cast<uint32_t*>(ptr) = key_len; ptr += 4;
  *reinterpret_cast<uint32_t*>(ptr) = value_len; ptr += 4;
  *reinterpret_cast<uint32_t*>(ptr) = entry.crc; ptr += 4;

  // 写入数据
  if (dbname_len > 0) {
    memcpy(ptr, entry.dbname.data(), dbname_len);
    ptr += dbname_len;
  }
  if (key_len > 0) {
    memcpy(ptr, entry.key.data(), key_len);
    ptr += key_len;
  }
  if (value_len > 0) {
    memcpy(ptr, entry.value.data(), value_len);
    ptr += value_len;
  }

  // 写入文件
  ssize_t written = write(fd, buf.data(), total_len);
  if (written != static_cast<ssize_t>(total_len)) {
    LOG(ERROR) << "Failed to write wal entry, written: " << written << ", expected: " << total_len;
    return false;
  }

  *offset += total_len;
  return true;
}

// V2版本：按长度读取，对应WriteEntryV2的格式
bool WAL::ReadEntryV2(int fd, LogEntry* entry, uint64_t* offset) {
  // 读取头部
  uint8_t header[kEntryHeaderSizeV2];
  ssize_t read_size = pread(fd, header, kEntryHeaderSizeV2, *offset);
  if (read_size != static_cast<ssize_t>(kEntryHeaderSizeV2)) {
    if (read_size == 0) {
      return false; // 文件结束
    }
    if (read_size < 4) {
      return false; // 无法读取magic
    }
    // 检查magic是否有效
    uint32_t magic = *reinterpret_cast<uint32_t*>(header);
    if (magic != kMagicNumber) {
      return false;
    }
    LOG(ERROR) << "Failed to read wal header, read: " << read_size << ", expected: " << kEntryHeaderSizeV2;
    return false;
  }

  uint8_t* ptr = header;
  uint32_t magic = *reinterpret_cast<uint32_t*>(ptr); ptr += 4;
  if (magic != kMagicNumber) {
    return false; // 不是有效的日志条目
  }

  uint32_t total_len = *reinterpret_cast<uint32_t*>(ptr); ptr += 4;
  entry->type = static_cast<LogType>(*ptr); ptr += 1;
  entry->index = *reinterpret_cast<uint64_t*>(ptr); ptr += 8;
  entry->term = *reinterpret_cast<uint64_t*>(ptr); ptr += 8;
  uint32_t dbname_len = *reinterpret_cast<uint32_t*>(ptr); ptr += 4;
  uint32_t key_len = *reinterpret_cast<uint32_t*>(ptr); ptr += 4;
  uint32_t value_len = *reinterpret_cast<uint32_t*>(ptr); ptr += 4;
  entry->crc = *reinterpret_cast<uint32_t*>(ptr); ptr += 4;

  // 读取数据部分
  uint32_t data_len = dbname_len + key_len + value_len;
  if (data_len > 0) {
    std::vector<uint8_t> data(data_len);
    read_size = pread(fd, data.data(), data_len, *offset + kEntryHeaderSizeV2);
    if (read_size != static_cast<ssize_t>(data_len)) {
      LOG(ERROR) << "Failed to read wal data, read: " << read_size << ", expected: " << data_len;
      return false;
    }

    ptr = data.data();
    if (dbname_len > 0) {
      entry->dbname.assign(reinterpret_cast<const char*>(ptr), dbname_len);
      ptr += dbname_len;
    } else {
      entry->dbname.clear();
    }
    if (key_len > 0) {
      entry->key.assign(reinterpret_cast<const char*>(ptr), key_len);
      ptr += key_len;
    } else {
      entry->key.clear();
    }
    if (value_len > 0) {
      entry->value.assign(reinterpret_cast<const char*>(ptr), value_len);
      ptr += value_len;
    } else {
      entry->value.clear();
    }
  } else {
    entry->dbname.clear();
    entry->key.clear();
    entry->value.clear();
  }

  // 校验CRC
  uint32_t calc_crc = CalculateCRC(*entry);
  if (calc_crc != entry->crc) {
    LOG(ERROR) << "CRC mismatch for log entry " << entry->index << ", calculated: " << calc_crc << ", stored: " << entry->crc;
    return false;
  }

  *offset += total_len;
  return true;
}

// 旧版本的WriteEntry（保留但标记为废弃）
bool WAL::WriteEntry(int fd, const LogEntry& entry, uint64_t* offset) {
  return WriteEntryV2(fd, entry, offset);
}

// 旧版本的ReadEntry（保留但标记为废弃）
bool WAL::ReadEntry(int fd, LogEntry* entry, uint64_t* offset) {
  return ReadEntryV2(fd, entry, offset);
}

bool WAL::AppendLog(const LogEntry& entry, bool sync) {
  base::MutexLock lock(&mutex_);
  if (current_fd_ < 0) {
    LOG(ERROR) << "WAL not initialized";
    return false;
  }

  // 检查是否需要滚动段文件
  uint32_t entry_size = kEntryHeaderSizeV2 + 
                        static_cast<uint32_t>(entry.dbname.size()) + 
                        static_cast<uint32_t>(entry.key.size()) + 
                        static_cast<uint32_t>(entry.value.size());
  if (current_offset_ + entry_size > segment_size_) {
    if (!RotateSegment()) {
      return false;
    }
  }

  // 计算CRC
  LogEntry entry_with_crc = entry;
  entry_with_crc.crc = CalculateCRC(entry);

  // 记录写入前的偏移（用于索引）
  uint64_t entry_offset = current_offset_;

  // 写入日志
  if (!WriteEntryV2(current_fd_, entry_with_crc, &current_offset_)) {
    return false;
  }

  // 刷盘
  if (sync) {
    if (!Sync()) {
      return false;
    }
  }

  // 更新内存索引
  log_index_[entry.index] = LogPosition(current_segment_id_, entry_offset, entry.term);

  // 更新最后日志信息
  last_index_ = entry.index;
  last_term_ = entry.term;

  VLOG(5) << "AppendLog: index=" << entry.index << ", term=" << entry.term 
          << ", segment=" << current_segment_id_ << ", offset=" << entry_offset;

  return true;
}

bool WAL::Sync() {
  if (current_fd_ < 0) {
    return false;
  }
  if (fdatasync(current_fd_) != 0) {
    LOG(ERROR) << "Failed to sync wal file: " << strerror(errno);
    return false;
  }
  return true;
}

// 构建内存索引：扫描所有段文件，建立index到(segment_id, offset, term)的映射
bool WAL::BuildIndex() {
  log_index_.clear();
  uint64_t max_index = 0;
  uint64_t max_term = 0;

  // 扫描所有段文件，按顺序读取
  for (uint64_t seg_id = 1; seg_id <= current_segment_id_; seg_id++) {
    int fd = OpenSegment(seg_id, false);
    if (fd < 0) {
      LOG(WARNING) << "Skip missing wal segment: " << seg_id;
      continue;
    }

    uint64_t offset = 0;
    LogEntry entry;
    while (ReadEntryV2(fd, &entry, &offset)) {
      // 记录索引位置（offset - 这条日志的长度 = 这条日志的起始偏移）
      // 但更简单的是在读取前记录offset
      uint64_t entry_offset = offset - (kEntryHeaderSizeV2 + 
                        static_cast<uint32_t>(entry.dbname.size()) + 
                        static_cast<uint32_t>(entry.key.size()) + 
                        static_cast<uint32_t>(entry.value.size()));
      
      // 实际上我们需要重新读取，让我们简化：重新扫描，这次同时记录位置
      // 这里我们只记录index和term，位置可以简化处理
      // 更好的做法是：关闭fd，重新扫描并记录位置
      
      // 暂时简化：只记录index和term
      log_index_[entry.index] = LogPosition(seg_id, 0, entry.term);
      
      if (entry.index > max_index) {
        max_index = entry.index;
        max_term = entry.term;
      }
    }
    close(fd);
  }

  // 重新扫描，这次记录准确的偏移位置
  // 这是一个简化实现，实际生产环境可以优化
  for (uint64_t seg_id = 1; seg_id <= current_segment_id_; seg_id++) {
    int fd = OpenSegment(seg_id, false);
    if (fd < 0) {
      continue;
    }

    uint64_t offset = 0;
    LogEntry entry;
    while (true) {
      uint64_t entry_start_offset = offset;
      if (!ReadEntryV2(fd, &entry, &offset)) {
        break;
      }
      // 更新准确的偏移位置
      if (log_index_.find(entry.index) != log_index_.end()) {
        log_index_[entry.index].offset = entry_start_offset;
      }
    }
    close(fd);
  }

  last_index_ = max_index;
  last_term_ = max_term;
  LOG(INFO) << "WAL BuildIndex completed, total entries: " << log_index_.size() 
            << ", last index: " << max_index << ", last term: " << max_term;
  return true;
}

bool WAL::Recovery(uint64_t* last_index, uint64_t* last_term) {
  base::MutexLock lock(&mutex_);
  
  // 构建内存索引
  BuildIndex();
  
  *last_index = last_index_;
  *last_term = last_term_;
  LOG(INFO) << "WAL recovery completed, last index: " << last_index_ << ", last term: " << last_term_;
  return true;
}

// 按索引获取单条日志
bool WAL::GetLogEntry(uint64_t index, LogEntry* entry) {
  base::MutexLock lock(&mutex_);
  
  base::hash_map<uint64_t, LogPosition>::iterator it = log_index_.find(index);
  if (it == log_index_.end()) {
    VLOG(5) << "GetLogEntry: index " << index << " not found in index";
    return false;
  }
  
  LogPosition& pos = it->second;
  int fd = OpenSegment(pos.segment_id, false);
  if (fd < 0) {
    LOG(ERROR) << "GetLogEntry: failed to open segment " << pos.segment_id;
    return false;
  }
  
  uint64_t offset = pos.offset;
  bool result = ReadEntryV2(fd, entry, &offset);
  close(fd);
  
  if (result && entry->index != index) {
    LOG(ERROR) << "GetLogEntry: index mismatch, expected " << index << ", got " << entry->index;
    return false;
  }
  
  return result;
}

// 按索引获取日志的term（快速查询，不需要读取整个日志）
bool WAL::GetLogTerm(uint64_t index, uint64_t* term) {
  base::MutexLock lock(&mutex_);
  
  base::hash_map<uint64_t, LogPosition>::iterator it = log_index_.find(index);
  if (it == log_index_.end()) {
    VLOG(5) << "GetLogTerm: index " << index << " not found in index";
    return false;
  }
  
  *term = it->second.term;
  return true;
}

void WAL::GetLastLogInfo(uint64_t* index, uint64_t* term) {
  base::MutexLock lock(&mutex_);
  *index = last_index_;
  *term = last_term_;
}

bool WAL::TruncateTo(uint64_t index) {
  base::MutexLock lock(&mutex_);
  
  LOG(INFO) << "Truncate WAL to index: " << index;
  
  // 如果index >= last_index_，不需要截断
  if (index >= last_index_) {
    LOG(INFO) << "Index " << index << " is >= last_index_ " << last_index_ << ", no truncation needed";
    return true;
  }
  
  // 找到包含index的日志位置
  base::hash_map<uint64_t, LogPosition>::iterator it = log_index_.find(index);
  if (it == log_index_.end()) {
    LOG(ERROR) << "Index " << index << " not found in WAL index";
    return false;
  }
  
  LogPosition& pos = it->second;
  uint64_t truncate_segment_id = pos.segment_id;
  uint64_t truncate_offset = pos.offset;
  
  // 1. 截断当前段文件（如果index在当前段）
  if (truncate_segment_id == current_segment_id_) {
    // 计算截断后的文件大小
    // 需要找到index这条日志的结束位置
    // 简化实现：我们需要读取这条日志来确定它的结束位置
    int fd = OpenSegment(truncate_segment_id, true);
    if (fd < 0) {
      LOG(ERROR) << "Failed to open segment " << truncate_segment_id << " for truncation";
      return false;
    }
    
    // 读取这条日志来确定它的长度
    LogEntry entry;
    uint64_t offset = truncate_offset;
    if (!ReadEntryV2(fd, &entry, &offset)) {
      LOG(ERROR) << "Failed to read entry at index " << index;
      close(fd);
      return false;
    }
    
    // offset现在指向这条日志之后的位置，这就是我们要截断的位置
    if (ftruncate(fd, truncate_offset) != 0) {
      LOG(ERROR) << "Failed to truncate segment " << truncate_segment_id << ": " << strerror(errno);
      close(fd);
      return false;
    }
    
    // 更新current_offset_
    current_offset_ = truncate_offset;
    close(fd);
  }
  
  // 2. 删除所有段ID大于truncate_segment_id的段文件
  // 先收集要删除的段
  std::vector<uint64_t> segments_to_delete;
  for (base::hash_map<uint64_t, LogPosition>::iterator idx_it = log_index_.begin();
       idx_it != log_index_.end(); ++idx_it) {
    if (idx_it->first > index) {
      uint64_t seg_id = idx_it->second.segment_id;
      if (seg_id > truncate_segment_id) {
        bool already_added = false;
        for (size_t i = 0; i < segments_to_delete.size(); i++) {
          if (segments_to_delete[i] == seg_id) {
            already_added = true;
            break;
          }
        }
        if (!already_added && seg_id != current_segment_id_) {
          segments_to_delete.push_back(seg_id);
        }
      }
    }
  }
  
  // 删除段文件
  for (size_t i = 0; i < segments_to_delete.size(); i++) {
    uint64_t seg_id = segments_to_delete[i];
    std::string file_path = wal_dir_ + "/wal_" + std::to_string(seg_id);
    if (unlink(file_path.c_str()) != 0) {
      LOG(WARNING) << "Failed to delete WAL segment " << seg_id << ": " << strerror(errno);
    } else {
      LOG(INFO) << "Deleted WAL segment during truncation: " << file_path;
    }
  }
  
  // 3. 从内存索引中删除所有大于index的条目
  base::hash_map<uint64_t, LogPosition>::iterator idx_it = log_index_.begin();
  while (idx_it != log_index_.end()) {
    if (idx_it->first > index) {
      log_index_.erase(idx_it++);
    } else {
      ++idx_it;
    }
  }
  
  // 4. 更新last_index_和last_term_
  last_index_ = index;
  
  // 找到index对应的term
  base::hash_map<uint64_t, LogPosition>::iterator term_it = log_index_.find(index);
  if (term_it != log_index_.end()) {
    last_term_ = term_it->second.term;
  }
  
  LOG(INFO) << "WAL truncation completed, last_index=" << last_index_ << ", last_term=" << last_term_;
  
  return true;
}

bool WAL::ReadFrom(uint64_t index, void (*callback)(const LogEntry&)) {
  base::MutexLock lock(&mutex_);
  // 遍历所有段，从index开始读取日志
  for (uint64_t seg_id = 1; seg_id <= current_segment_id_; seg_id++) {
    int fd = OpenSegment(seg_id, false);
    if (fd < 0) continue;

    uint64_t offset = 0;
    LogEntry entry;
    while (ReadEntryV2(fd, &entry, &offset)) {
      if (entry.index >= index) {
        callback(entry);
      }
    }
    close(fd);
  }
  return true;
}

bool WAL::CleanOldLogs(uint64_t commit_index, uint64_t keep_count) {
  base::MutexLock lock(&mutex_);
  uint64_t keep_from = commit_index > keep_count ? commit_index - keep_count : 0;
  LOG(INFO) << "Clean old WAL logs before index: " << keep_from;
  
  if (keep_from == 0) {
    return true;
  }
  
  // 收集每个段文件包含的最小和最大索引
  base::hash_map<uint64_t, std::pair<uint64_t, uint64_t>> segment_index_range;
  
  for (base::hash_map<uint64_t, LogPosition>::iterator it = log_index_.begin();
       it != log_index_.end(); ++it) {
    uint64_t index = it->first;
    uint64_t segment_id = it->second.segment_id;
    
    if (segment_index_range.find(segment_id) == segment_index_range.end()) {
      segment_index_range[segment_id] = std::make_pair(index, index);
    } else {
      if (index < segment_index_range[segment_id].first) {
        segment_index_range[segment_id].first = index;
      }
      if (index > segment_index_range[segment_id].second) {
        segment_index_range[segment_id].second = index;
      }
    }
  }
  
  // 找出可以删除的段文件（所有日志索引都小于keep_from）
  std::vector<uint64_t> segments_to_delete;
  for (base::hash_map<uint64_t, std::pair<uint64_t, uint64_t>>::iterator it = segment_index_range.begin();
       it != segment_index_range.end(); ++it) {
    uint64_t segment_id = it->first;
    uint64_t min_index = it->second.first;
    uint64_t max_index = it->second.second;
    
    // 如果段的最大索引小于keep_from，并且不是当前段，则可以删除
    if (max_index < keep_from && segment_id != current_segment_id_) {
      segments_to_delete.push_back(segment_id);
      LOG(INFO) << "Marking segment " << segment_id << " for deletion (indices " 
                << min_index << "-" << max_index << ")";
    }
  }
  
  // 删除段文件并更新内存索引
  for (size_t i = 0; i < segments_to_delete.size(); i++) {
    uint64_t segment_id = segments_to_delete[i];
    std::string file_path = wal_dir_ + "/wal_" + std::to_string(segment_id);
    
    // 删除文件
    if (unlink(file_path.c_str()) != 0) {
      LOG(WARNING) << "Failed to delete WAL segment " << segment_id << ": " << strerror(errno);
    } else {
      LOG(INFO) << "Deleted WAL segment: " << file_path;
    }
    
    // 从内存索引中删除该段的所有条目
    base::hash_map<uint64_t, LogPosition>::iterator it = log_index_.begin();
    while (it != log_index_.end()) {
      if (it->second.segment_id == segment_id) {
        log_index_.erase(it++);
      } else {
        ++it;
      }
    }
  }
  
  return true;
}

} // namespace rafdb
