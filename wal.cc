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
// 日志条目头部大小：magic(4) + length(4) + type(1) + index(8) + term(8) + data_len(4) = 29字节
const uint32_t kEntryHeaderSize = 29;

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

bool WAL::WriteEntry(int fd, const LogEntry& entry, uint64_t* offset) {
  uint32_t data_len = entry.dbname.size() + entry.key.size() + entry.value.size();
  uint32_t total_len = kEntryHeaderSize + data_len + 4; // +4 for CRC

  // 分配缓冲区
  std::vector<uint8_t> buf(total_len);
  uint8_t* ptr = buf.data();

  // 写入头
  *reinterpret_cast<uint32_t*>(ptr) = kMagicNumber; ptr += 4;
  *reinterpret_cast<uint32_t*>(ptr) = total_len; ptr += 4;
  *ptr = static_cast<uint8_t>(entry.type); ptr += 1;
  *reinterpret_cast<uint64_t*>(ptr) = entry.index; ptr += 8;
  *reinterpret_cast<uint64_t*>(ptr) = entry.term; ptr += 8;
  *reinterpret_cast<uint32_t*>(ptr) = data_len; ptr +=4;

  // 写入数据
  memcpy(ptr, entry.dbname.data(), entry.dbname.size()); ptr += entry.dbname.size();
  memcpy(ptr, entry.key.data(), entry.key.size()); ptr += entry.key.size();
  memcpy(ptr, entry.value.data(), entry.value.size()); ptr += entry.value.size();

  // 写入CRC
  *reinterpret_cast<uint32_t*>(ptr) = entry.crc; ptr +=4;

  // 写入文件
  ssize_t written = write(fd, buf.data(), total_len);
  if (written != total_len) {
    LOG(ERROR) << "Failed to write wal entry, written: " << written << ", expected: " << total_len;
    return false;
  }

  *offset += total_len;
  return true;
}

bool WAL::ReadEntry(int fd, LogEntry* entry, uint64_t* offset) {
  // 读取头部
  uint8_t header[kEntryHeaderSize];
  ssize_t read_size = pread(fd, header, kEntryHeaderSize, *offset);
  if (read_size != kEntryHeaderSize) {
    if (read_size == 0) {
      return false; // 文件结束
    }
    LOG(ERROR) << "Failed to read wal header, read: " << read_size << ", expected: " << kEntryHeaderSize;
    return false;
  }

  uint8_t* ptr = header;
  uint32_t magic = *reinterpret_cast<uint32_t*>(ptr); ptr +=4;
  if (magic != kMagicNumber) {
    LOG(ERROR) << "Invalid wal magic number: " << magic << ", expected: " << kMagicNumber;
    return false;
  }

  uint32_t total_len = *reinterpret_cast<uint32_t*>(ptr); ptr +=4;
  entry->type = static_cast<LogType>(*ptr); ptr +=1;
  entry->index = *reinterpret_cast<uint64_t*>(ptr); ptr +=8;
  entry->term = *reinterpret_cast<uint64_t*>(ptr); ptr +=8;
  uint32_t data_len = *reinterpret_cast<uint32_t*>(ptr); ptr +=4;

  // 读取数据部分
  std::vector<uint8_t> data(data_len + 4); // +4 for CRC
  read_size = pread(fd, data.data(), data_len +4, *offset + kEntryHeaderSize);
  if (read_size != (ssize_t)(data_len +4)) {
    LOG(ERROR) << "Failed to read wal data, read: " << read_size << ", expected: " << data_len +4;
    return false;
  }

  // 解析数据
  ptr = data.data();
  uint32_t dbname_len = 0;
  // 实际实现中需要在LogEntry中记录各字段长度，这里简化处理
  // 暂时假设dbname、key、value都以\\0分隔，实际项目中应使用更可靠的序列化方式
  const char* dbname = reinterpret_cast<const char*>(ptr);
  entry->dbname = dbname;
  ptr += entry->dbname.size() + 1;

  const char* key = reinterpret_cast<const char*>(ptr);
  entry->key = key;
  ptr += entry->key.size() + 1;

  const char* value = reinterpret_cast<const char*>(ptr);
  entry->value = value;
  ptr += entry->value.size() + 1;

  entry->crc = *reinterpret_cast<uint32_t*>(ptr); ptr +=4;

  // 校验CRC
  uint32_t calc_crc = CalculateCRC(*entry);
  if (calc_crc != entry->crc) {
    LOG(ERROR) << "CRC mismatch for log entry " << entry->index << ", calculated: " << calc_crc << ", stored: " << entry->crc;
    return false;
  }

  *offset += total_len;
  return true;
}

bool WAL::AppendLog(const LogEntry& entry, bool sync) {
  base::MutexLock lock(&mutex_);
  if (current_fd_ < 0) {
    LOG(ERROR) << "WAL not initialized";
    return false;
  }

  // 检查是否需要滚动段文件
  if (current_offset_ + kEntryHeaderSize + entry.dbname.size() + entry.key.size() + entry.value.size() + 4 > segment_size_) {
    if (!RotateSegment()) {
      return false;
    }
  }

  // 计算CRC
  LogEntry entry_with_crc = entry;
  entry_with_crc.crc = CalculateCRC(entry);

  // 写入日志
  if (!WriteEntry(current_fd_, entry_with_crc, &current_offset_)) {
    return false;
  }

  // 刷盘
  if (sync) {
    if (!Sync()) {
      return false;
    }
  }

  // 更新最后日志信息
  last_index_ = entry.index;
  last_term_ = entry.term;

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

bool WAL::Recovery(uint64_t* last_index, uint64_t* last_term) {
  base::MutexLock lock(&mutex_);
  *last_index = 0;
  *last_term = 0;
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
    while (ReadEntry(fd, &entry, &offset)) {
      if (entry.index > max_index) {
        max_index = entry.index;
        max_term = entry.term;
      }
    }
    close(fd);
  }

  last_index_ = max_index;
  last_term_ = max_term;
  *last_index = max_index;
  *last_term = max_term;
  LOG(INFO) << "WAL recovery completed, last index: " << max_index << ", last term: " << max_term;
  return true;
}

void WAL::GetLastLogInfo(uint64_t* index, uint64_t* term) {
  base::MutexLock lock(&mutex_);
  *index = last_index_;
  *term = last_term_;
}

bool WAL::TruncateTo(uint64_t index) {
  base::MutexLock lock(&mutex_);
  // 找到包含index的段文件，截断后面的内容
  // 简化实现，实际需要遍历所有段找到对应位置
  LOG(INFO) << "Truncate WAL to index: " << index;
  // 这里暂时只更新last_index，实际实现需要修改文件内容
  last_index_ = index;
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
    while (ReadEntry(fd, &entry, &offset)) {
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
  // 实际实现需要删除所有只包含小于keep_from索引的段文件
  return true;
}

} // namespace rafdb
