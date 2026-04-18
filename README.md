# RafDB - 分布式Raft一致性Key-Value数据库

RafDB是一个基于Raft协议实现的分布式Key-Value存储系统，底层使用LevelDB作为存储引擎，支持强一致性和高可用。

## 目录

- [特性](#特性)
- [架构设计](#架构设计)
- [核心模块](#核心模块)
- [快速开始](#快速开始)
- [编译部署](#编译部署)
- [API文档](#api文档)
- [目录结构](#目录结构)

---

## 特性

### 一致性
- **Raft协议**：实现完整的Raft一致性算法，包括Leader选举、日志复制、安全性保证
- **强一致性**：写入操作需要多数节点确认后才返回成功
- **线性一致性读**：支持从Leader读取最新数据

### 高可用
- **自动选主**：Leader故障时自动选举新Leader
- **故障恢复**：支持节点故障后的日志重放和状态机恢复
- **数据持久化**：预写日志(WAL)确保数据不丢失

### 高性能
- **LevelDB存储**：基于Google LevelDB，支持高吞吐随机读写
- **批量操作**：支持批量写入和读取
- **异步复制**：日志复制异步进行，不阻塞主节点写入

### 可扩展性
- **一致性哈希**：客户端采用一致性哈希进行负载均衡
- **节点动态增删**：支持运行时动态添加/删除节点
- **分片支持**：支持数据分片存储（可扩展）

---

## 架构设计

```
                    ┌─────────────────────────────────────────────┐
                    │           客户端 (RafdbClient)               │
                    │  ┌─────────────────────────────────────┐   │
                    │  │     一致性哈希 (ConHashSelector)      │   │
                    │  └─────────────────────────────────────┘   │
                    └──────────────────────┬──────────────────────┘
                                           │
                ┌──────────────────────────┼──────────────────────────┐
                │                          │                          │
                ▼                          ▼                          ▼
        ┌──────────────┐           ┌──────────────┐           ┌──────────────┐
        │   Node A     │           │   Node B     │           │   Node C     │
        │  (Leader)    │◄─────────▶│  (Follower)  │◄─────────▶│  (Follower)  │
        └──────┬───────┘           └──────┬───────┘           └──────┬───────┘
               │                           │                           │
        ┌──────┴───────┐           ┌──────┴───────┐           ┌──────┴───────┐
        │   Raft层      │           │   Raft层      │           │   Raft层      │
        │  Accord模块   │           │  Accord模块   │           │  Accord模块   │
        └──────┬───────┘           └──────┬───────┘           └──────┬───────┘
               │                           │                           │
        ┌──────┴───────┐           ┌──────┴───────┐           ┌──────┴───────┐
        │  存储层       │           │  存储层       │           │  存储层       │
        │  LevelDB     │           │  LevelDB     │           │  LevelDB     │
        │  WAL日志      │           │  WAL日志      │           │  WAL日志      │
        └──────────────┘           └──────────────┘           └──────────────┘
```

### Raft协议流程

#### Leader选举
```
Follower等待超时 → 转换为Candidate → 发起选举请求 → 收到多数投票 → 成为Leader
         ↑                                                                 │
         └──────────── 收到心跳或投票被拒绝 ─────────────────────────────┘
```

#### 日志复制
```
1. 客户端写入请求 → Leader
2. Leader写入本地WAL → 发送AppendEntries到所有Follower
3. Follower写入本地WAL → 回复确认
4. Leader收到多数确认 → 提交日志 → 应用到状态机(写LevelDB) → 返回成功给客户端
5. Leader在下一次AppendEntries中通知Follower提交
```

---

## 核心模块

### 1. Accord模块 (`accord.cc/accord.h`)
Raft协议的核心实现，包括：

- **状态管理**：Follower/Candidate/Leader三种状态转换
- **Leader选举**：随机超时机制、投票处理、任期管理
- **日志复制**：AppendEntries RPC、日志匹配、提交确认
- **一致性保证**：日志完整性检查、任期验证

### 2. RafDb模块 (`rafdb.cc/rafdb.h`)
数据库的核心接口层，包括：

- **读写接口**：`Set()`/`Get()`/`LSet()`
- **Raft封装**：`RaftSet()` 实现Raft写入流程
- **数据同步**：与WAL和LevelDB的交互
- **状态管理**：节点健康检查、Leader状态跟踪

### 3. WAL模块 (`wal.cc/wal.h`)
预写日志系统，确保数据持久化：

- **日志格式**：带校验和(CRC32)的二进制格式
- **分段存储**：支持128MB分段，自动滚动
- **内存索引**：快速日志定位和term查询
- **故障恢复**：启动时扫描WAL重建内存索引

### 4. Peer模块 (`peer.cc/peer.h`)
Leader节点的心跳和健康管理：

- **心跳发送**：定期向Follower发送心跳
- **健康检测**：检测断开连接的节点数量
- **状态同步**：通知Raft层健康状态变化

### 5. 客户端模块 (`client/rafdb_client.cc`)
RafDB的客户端实现：

- **一致性哈希**：`ConHashSelector` 实现负载均衡
- **节点感知**：自动检测Leader节点
- **API封装**：提供简单的读写接口

---

## 快速开始

### 环境要求
- Linux/macOS/Windows
- C++编译器（支持C++11或更高）
- Thrift 0.9.x 或更高版本
- LevelDB
- base库（项目依赖的基础库）

### 编译

```bash
# 使用ymake编译
cd rafdb
../../../devel/ymake/ymake.sh t=rafdb

# 或者使用make
make
```

### 启动集群

#### 方式一：使用脚本启动
```bash
# 启动3节点集群
./run_rafdb_main.sh start
```

#### 方式二：手动启动各节点

```bash
# 节点1
./rafdb_main --port=9001 --self_id=1 --node_list="127.0.0.1:9002,127.0.0.1:9003"

# 节点2
./rafdb_main --port=9002 --self_id=2 --node_list="127.0.0.1:9001,127.0.0.1:9003"

# 节点3
./rafdb_main --port=9003 --self_id=3 --node_list="127.0.0.1:9001,127.0.0.1:9002"
```

### 使用客户端

```cpp
#include "storage/rafdb/client/rafdb_client.h"

int main() {
    // 创建客户端
    RafdbClient client;
    
    // 添加节点
    client.AddNode("127.0.0.1", 9001);
    client.AddNode("127.0.0.1", 9002);
    client.AddNode("127.0.0.1", 9003);
    
    // 初始化
    client.Init();
    
    // 写入数据（使用Raft一致性）
    std::string value;
    client.LSet("mydb", "key1", "value1");
    
    // 读取数据
    client.Get("mydb", "key1", &value);
    
    return 0;
}
```

---

## 编译部署

### 目录说明
```
rafdb/
├── client/          # 客户端实现
├── proto/           # Thrift协议定义
├── test/            # 测试脚本
├── README.md        # 本文档
├── accord.cc/h      # Raft协议核心实现
├── global.h         # 全局定义
├── manager.cc/h     # 管理模块（已弃用，使用Raft）
├── peer.cc/h        # Leader心跳管理
├── rafdb.cc/h       # 数据库核心接口
├── rafdb_main.cc    # 主程序入口
├── rafdb_sync.cc/h  # 同步客户端（用于节点间通信）
├── sync.cc/h        # 旧同步模块（已弃用）
├── wal.cc/h         # 预写日志实现
└── YBUILD           # ymake构建配置
```

### 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--port` | 无 | 节点监听端口 |
| `--self_id` | 无 | 节点唯一ID（整数） |
| `--node_list` | 无 | 其他节点列表，逗号分隔 |
| `--wal_dir` | ./wal | WAL日志存储目录 |
| `--data_dir` | ./data | LevelDB数据目录 |

### Raft参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| 选举超时 | 3000-6000ms | 随机选举超时时间 |
| 心跳间隔 | 100ms | Leader发送心跳的间隔 |
| Quorum大小 | N/2 + 1 | 需要确认的节点数 |

---

## API文档

### RafDb接口

#### LSet - Raft一致性写入
```cpp
/**
 * 使用Raft协议写入数据（强一致性）
 * 需要多数节点确认后才返回
 * 
 * @param dbname 数据库名称
 * @param key 键
 * @param value 值
 * @return true 写入成功
 *         false 写入失败（非Leader或超时）
 */
bool LSet(const std::string &dbname, 
          const std::string &key, 
          const std::string &value);
```

#### Set - 直接写入（不推荐）
```cpp
/**
 * 直接写入LevelDB，不经过Raft
 * 注意：此方法不保证一致性，仅用于内部
 */
bool Set(const std::string &dbname, 
         const std::string &key, 
         const std::string &value);
```

#### Get - 读取数据
```cpp
/**
 * 从本地LevelDB读取数据
 * 注意：从Follower读取可能是旧数据
 * 要获取最新数据，应从Leader读取
 */
bool Get(const std::string &key, 
         const std::string &dbname,
         std::string &value);
```

### 客户端接口

#### RafdbClient::LSet
```cpp
/**
 * 客户端Raft写入接口
 * 自动找到Leader并写入
 */
bool LSet(const std::string &dbname, 
          const std::string &key, 
          const std::string &value);
```

#### RafdbClient::Get
```cpp
/**
 * 客户端读取接口
 * 使用一致性哈希选择节点
 */
bool Get(const std::string &dbname, 
         const std::string &key, 
         std::string &value);
```

---

## Raft协议实现细节

### 日志条目结构
```cpp
struct LogEntry {
    i64 index;           // 日志索引
    i64 term;            // 日志任期
    LogType type;        // 日志类型（NORMAL/CONFIG/NOOP）
    string dbname;       // 数据库名
    string key;          // 键
    string value;        // 值
    i32 crc;             // CRC32校验和
};
```

### 消息类型

| 消息类型 | 方向 | 说明 |
|---------|------|------|
| VOTEREQ | Candidate → All | 请求投票 |
| VOTEREP | Follower → Candidate | 投票回复 |
| HEARTREQ | Leader → All | 心跳（空AppendEntries） |
| HEARTREP | Follower → Leader | 心跳回复 |
| APPENDENTRIESREQ | Leader → Follower | 日志复制请求 |
| APPENDENTRIESREP | Follower → Leader | 日志复制回复 |
| LEADERREQ | Any → All | 查询Leader |
| LEADERREP | Any → Requester | Leader查询回复 |

### 安全性保证

RafDB实现了Raft协议的所有安全性保证：

1. **选举安全**：一个任期内最多只有一个Leader
2. **Leader只追加**：Leader只追加日志，不删除或覆盖
3. **日志匹配**：如果两个日志有相同的index和term，则前面所有日志都相同
4. **Leader完整性**：如果一条日志在某个任期被提交，它将出现在所有更高任期的Leader的日志中
5. **状态机安全**：如果一个服务器在某个index应用了日志，其他服务器不会在同一个index应用不同的日志

---

## 故障处理

### Leader故障
1. Follower检测到心跳超时（3000-6000ms）
2. 转换为Candidate，增加任期，发起选举
3. 收到多数投票后成为新Leader
4. 发送空AppendEntries（心跳）确立权威

### Follower故障
1. Leader检测到AppendEntries失败
2. 记录断开节点数
3. 如果断开节点数 >= Quorum，Leader退位（避免脑裂）
4. 节点恢复后，Leader通过AppendEntries同步缺失的日志

### 数据恢复
1. 节点重启时扫描WAL
2. 重建内存索引（index → segment_id, offset, term）
3. 找到最后一条日志的index和term
4. 加入集群，通过AppendEntries同步缺失的日志

---

## 性能优化建议

1. **WAL分段大小**：根据实际写入量调整，默认128MB
2. **LevelDB配置**：可以调整block_size、write_buffer_size等参数
3. **网络优化**：使用低延迟网络，减少Raft往返时间
4. **批量写入**：如果业务允许，可以批量提交减少RPC次数
5. **读优化**：读多写少场景可以考虑从Follower读取（牺牲一致性）

---

## 注意事项

1. **集群规模**：建议使用奇数节点（3、5、7...），避免脑裂
2. **数据一致性**：LSet是强一致性，Get可能从本地读取旧数据
3. **网络分区**：Raft协议可以处理网络分区，但恢复时间取决于选举超时
4. **持久化**：WAL必须同步刷盘，否则可能丢失数据
5. **资源占用**：Raft协议需要额外的CPU和网络资源

---

## 联系方式

如有问题或建议，请联系：`zxl051_1@126.com`

---

## 版本历史

| 版本 | 日期 | 说明 |
|------|------|------|
| 1.0 | - | 初始版本，基于master/slave架构 |
| 2.0 | 2025-04 | 实现完整Raft协议，支持强一致性 |

---

## 许可证

（待补充）
