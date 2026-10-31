# 点赞系统设计方案

## 1. 架构与保证

采用 **Redis + Lua + Redis Stream + RocketMQ + MySQL**：Redis 承接在线读写，Stream 保存待投递事件，RocketMQ 削峰与重试，MySQL 保存最终状态。

```text
客户端 → Gin API → Redis Lua（状态 + 计数 + Stream 事件）→ 返回结果
                                  ↓
                             Relay 转发服务
                                  ↓
                              RocketMQ
                                  ↓
                        Consumer → MySQL 事务
```

- 同一用户对同一对象只有一个点赞状态，支持文章、视频、评论。
- Redis 内正常执行的操作原子可见；事件至少投递一次，MySQL 通过版本号实现幂等。
- 在线查询读取 Redis 主节点，可立即看到成功写入；MySQL 最终一致，异步期间允许滞后。
- 最终一致的前提是事件未丢失、失败事件最终被处理，且版本号不回退。

**持久化边界：** Stream 解决 Redis 与 MQ 双写窗口，不能消除 Redis 自身的数据丢失风险。AOF `everysec` 在故障时仍可能丢失约一秒数据；复制确认也不等于强一致。若业务要求成功响应后严格不丢，应选择 MySQL 事务写状态及 Outbox，再异步更新 Redis、投递 MQ。[Redis 持久化](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)、[WAIT 的一致性边界](https://redis.io/docs/latest/commands/wait/)

## 2. MySQL 表设计

沿用点赞关系表与计数表，版本号属于 `(user_id, target_type, target_id)`，不是对象的全局版本。

```sql
CREATE TABLE user_like (
    id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    user_id BIGINT UNSIGNED NOT NULL,
    target_id BIGINT UNSIGNED NOT NULL,
    target_type TINYINT UNSIGNED NOT NULL COMMENT '1文章 2视频 3评论',
    liked TINYINT UNSIGNED NOT NULL DEFAULT 0,
    version BIGINT UNSIGNED NOT NULL DEFAULT 0,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
        ON UPDATE CURRENT_TIMESTAMP(3),
    UNIQUE KEY uk_user_target (user_id, target_type, target_id),
    KEY idx_target (target_type, target_id, liked)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE like_count (
    target_id BIGINT UNSIGNED NOT NULL,
    target_type TINYINT UNSIGNED NOT NULL,
    like_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
        ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (target_type, target_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

取消点赞只更新 `liked=0`，保留记录及版本，防止旧消息重新生效。接口与消费者均校验对象类型、状态及版本范围；对象创建时初始化计数行。

## 3. Redis 数据结构

以文章 `2001` 为例，统一使用数字类型构造 Hash Tag `{1:2001}`：

`{1:2001}` 是 Redis Cluster 的哈希标签，其中 `1` 表示文章类型、`2001` 表示对象 ID，Redis 仅使用花括号内的内容计算槽位，使该对象的相关 Key 落入同一槽位，支持 Lua 原子操作。

| 类型 | Key | 内容 |
| --- | --- | --- |
| Set | `like:{1:2001}:users` | 当前已点赞用户 ID |
| String | `like:{1:2001}:count` | 点赞总数 |
| Hash | `like:{1:2001}:versions` | 用户 ID → 当前版本，包括已取消的用户 |
| Stream | `like:{1:2001}:events` | 待转发的状态变更事件 |
| Hash | `like:{1:2001}:meta` | 初始化状态 `READY/RECOVERING`、重建代次 `generation` |

所有 Lua Key 显式通过 `KEYS` 传入，并使用相同 Hash Tag，满足 Redis Cluster 的同槽要求。不同对象的 Stream 分别读取，不能直接跨槽执行一次多 Stream `XREADGROUP`。

这些数据承担在线状态和事件日志职责，使用独立 Redis、`noeviction`，不设置独立 TTL。空 Set、空 Hash 可能不存在，只有 `meta=READY` 才能将缺失成员解释为未点赞、版本零；`count` 必须存在。正常状态满足 `count = SCARD(users)`。

部署时启用 AOF、主从复制及持久化故障告警，禁止其他业务修改或删除这些 Key；权限、内存和磁盘容量在接收写入前检查。Redis 故障切换须与入口停写联动，完成恢复后才放行，不能仅依赖客户端自动重连。

## 4. API 与请求幂等

| 接口 | 作用 |
| --- | --- |
| `PUT /api/v1/targets/:type/:id/like` | 点赞或取消点赞，Body：`{"action":"LIKE"}` 或 `{"action":"UNLIKE"}` |
| `GET /api/v1/targets/:type/:id/like` | 查询当前用户状态、版本、对象计数 |
| `POST /api/v1/likes/batch-query`（可选） | 列表场景批量查询，限制单次对象数量 |

用户 ID 从登录凭证获取；校验对象存在、可访问并允许点赞。返回 `liked`、`count`、`version`、`changed`，ID、计数和版本统一用十进制字符串传输，避免客户端整数精度丢失。

`changed` 用于点赞或取消点赞的响应，表示本次请求是否实际改变了点赞状态：

| 操作 | 操作前状态 | changed |
| --- | --- | --- |
| `LIKE` | 未点赞 | `true` |
| `LIKE` | 已点赞 | `false` |
| `UNLIKE` | 已点赞 | `true` |
| `UNLIKE` | 未点赞 | `false` |

- `true`：状态发生变化，更新计数、版本号，并生成事件。
- `false`：当前状态已符合请求，无须修改，也不生成事件，不代表请求失败。

例如，重复点赞返回 `liked=true, changed=false`，表示当前已点赞，但这次请求没有产生新的变更。

沿用 `LIKE / UNLIKE` 操作，客户端不传版本号，由 Redis 判断当前状态：

1. `LIKE` 且已经点赞：返回 `ALREADY_LIKED`，`changed=false`。
2. `UNLIKE` 且尚未点赞：返回 `NOT_LIKED`，`changed=false`。
3. 其余情况执行状态变更，由 Redis 将版本加一并生成事件；前两种情况均不增加版本、不产生事件。

幂等性以 Redis 当前状态为依据，操作按 Redis 实际执行顺序生效；版本号用于 MySQL 消费端处理消息重复和乱序。客户端串行发送同一对象的操作，超时后先查询状态，避免旧操作重试与新操作交错。

超时后的查询不能证明原请求已结束；延迟的 LIKE 如果在 UNLIKE 之后执行，仍会再次点赞。本方案保证状态幂等与消费幂等，不保证跨相反操作的旧请求去重或客户端意图顺序。

## 5. Lua 原子写入

以下为流程伪代码，实际脚本应完成全部校验后再执行第一条写命令：

```text
校验 action 为 LIKE 或 UNLIKE、其他参数、Key 类型、meta=READY、count 存在且合法
ID 保持字符串；计数、版本须为规范非负十进制整数，且不超过 9007199254740991
读取 SISMEMBER(users, user_id)、HGET(versions, user_id)
校验 count = SCARD(users)、当前版本合法；已点赞用户必须存在正版本
在完整初始化且未损坏的数据中，未点赞且版本字段不存在时使用默认版本 0
若 action=LIKE 且已点赞：返回 ALREADY_LIKED，changed=false
若 action=UNLIKE 且未点赞：返回 NOT_LIKED，changed=false
校验取消时 count > 0，点赞时计数未达上限，版本未达上限
计算 new_version、new_count
使用 string.format("%.0f", value) 将新版本、新计数转为十进制字符串
设置 meta=RECOVERING，阻止写入中途失败后继续服务该对象
XADD events * user_id ... target_id ... target_type ...
             action ... version new_version generation ...
按 LIKE/UNLIKE 执行 SADD/SREM users
SET count new_count
HSET versions user_id new_version
设置 meta=READY
返回当前状态、new_count、new_version、Stream ID，changed=true

设置 RECOVERING 失败：停止执行，尚未修改状态和事件
此后任一步失败：停止后续步骤，保持 RECOVERING，按第 9 节恢复
```

成功 XADD 的事件作为恢复依据：即使后续状态写入失败，也继续投递该事件并最终恢复到其目标状态；事件未写入时保留原状态。接口超时或写入阶段异常返回结果未知，不声称操作已回滚。

## 6. Stream → RocketMQ

每个对象一个 Stream，使用固定消费组 `like-relay`，多个 Relay 分工处理：

1. 使用 `XGROUP CREATE ... 0 MKSTREAM` 建组，从头读取已有事件；重复建组的 `BUSYGROUP` 可忽略。
2. `XREADGROUP ... STREAMS key >` 读取新事件，同步发送 MQ。
3. 只有获得符合所选 Broker 持久化、复制策略的成功确认后，才执行 `XACK`；超时或失败保留 pending，持续重试。
4. 定期通过 `XPENDING` 监控，使用 `XAUTOCLAIM` 接管超时 pending，接管阈值应大于正常发送耗时。发送成功、ACK 前宕机会重复投递，由消费者幂等处理。[Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/)

MQ 消息示例：

```json
{
  "schema_version": 1,
  "event_id": "1:2001:g7:1793451600000-0",
  "generation": "g7",
  "stream_id": "1793451600000-0",
  "user_id": "1001",
  "target_id": "2001",
  "target_type": 1,
  "action": "LIKE",
  "version": "3"
}
```

`event_id` 由类型、对象、重建代次、Stream ID 组成，重发时不变；时间可从 Stream ID 提取，只用于观测，不参与新旧判断。`generation` 用于区分日志生命周期，业务版本仍必须单调递增。

Relay 的对象发现不能只依赖一次通知：以持久化的对象目录分配任务，并周期性扫描各 Redis 主节点的 Stream 补漏。MQ 长时间不可用时，根据积压量限流或暂停新写入，避免 Redis 内存耗尽。SDK 内置重试耗尽后仍须保留 pending。[RocketMQ 发送重试](https://rocketmq.apache.org/docs/featureBehavior/05sendretrypolicy/)

`XACK` 仅解除 pending，不代表 MySQL 已落库，也不删除事件。[Redis XACK](https://redis.io/docs/latest/commands/xack/)

初版仅保留 Relay 一个消费组。清理任务读取 MySQL 主库，只有对应关系的 `db.version >= event.version`，才用同槽 Lua 执行 `XACK + XDEL`；版本相等时还需校验状态相同，否则告警并保留事件。未落库事件持续保留，超过延迟阈值则按原事件 ID 重新投递，包括已经 ACK 的事件。清理任务与重建任务互斥，禁止使用固定 `MAXLEN` 裁剪日志。

MQ 保留期需覆盖预期故障与恢复时间，但过期清理不以消息是否消费为依据，因此仍以 MySQL 版本核对和 Stream 补投作为恢复手段。[RocketMQ 存储与清理](https://rocketmq.apache.org/docs/featureBehavior/11messagestorepolicy/)

## 7. RocketMQ → MySQL

使用“最新状态 + 单调版本”，允许重复、乱序、版本跳跃；不直接应用消息中的 `+1/-1`。

每条消息执行一个事务，所有消费者采用相同加锁顺序：

1. 校验消息结构、对象类型、action 和版本；仅接受 `LIKE / UNLIKE`，分别映射为 `newLiked=1/0`，异常消息进入故障处理流程。
2. 插入默认关系 `(liked=0, version=0)`，冲突时保持原值；按唯一键 `SELECT liked, version ... FOR UPDATE`。
3. 若 `msg.version < db.version`，提交空变更并确认消费；若版本相同且状态相同，也直接确认，版本相同但状态不同则告警并隔离该对象，检查版本回退或消息损坏。
4. 否则计算 `delta = int(newLiked) - int(dbLiked)`，更新关系状态及版本；`delta=0` 时仍要更新版本。
5. 校验计数行存在；`delta != 0` 时执行对应安全 SQL，必须检查受影响行数为 1。
6. 提交成功后确认消费；任何错误或死锁均回滚整个事务并重试。

计数行由对象创建流程初始化；消费时缺失则回滚、告警并进入修复流程，不在消费者中补零。历史对象须先按关系表初始化计数，再接入写入链路。

```sql
-- delta = +1
UPDATE like_count SET like_count = like_count + 1
WHERE target_type = ? AND target_id = ?
  AND like_count < 9007199254740991;

-- delta = -1
UPDATE like_count SET like_count = like_count - 1
WHERE target_type = ? AND target_id = ? AND like_count > 0;
```

分支 SQL 避免对无符号列直接加负数；异常不允许用截断到零的方式掩盖。[MySQL 整数运算规则](https://dev.mysql.com/doc/refman/8.4/en/arithmetic-functions.html)

例如数据库为未点赞 v0，先收到取消 v2：只更新版本，计数不变；后收到点赞 v1：忽略。若最终收到点赞 v3：计数加一。计数变化取决于数据库旧状态与消息新状态，因此能容忍乱序。

MQ 重试耗尽进入死信队列后，需要告警、修复并重放，才能恢复最终一致；不得直接丢弃或将失败返回成功。[RocketMQ 消费重试](https://rocketmq.apache.org/docs/featureBehavior/10consumerretrypolicy/)

## 8. 查询路径

- 单对象：用只读 Lua 一次读取 `meta`、`SISMEMBER`、`GET count`、`HGET version`，获得同一时点结果。
- 批量：按对象分组，通过 Cluster 客户端 Pipeline 发往对应节点；不承诺跨对象快照一致。
- `meta` 非 READY、计数缺失或 Redis 不可用：返回暂不可用，或显式返回带 `stale=true` 的 MySQL 结果；不将异步落后的数据写回 Redis。

## 9. 初始化、故障恢复与对账

**新对象：** 在对象创建流程中创建 MySQL 计数行，初始化 Redis `count=0`、唯一 `generation` 和消费组，最后设置 `meta=READY`。初始化须互斥、可重试，已 READY 的对象不能再次清零；只有确认无历史数据、无待处理事件的新对象才能直接补零。

**已有对象重建：**

1. 在 API 路由层暂停该对象写入、等待在途写结束，再进入 RECOVERING；重建任务互斥且带 fencing token，防止失效任务发布旧快照。
   若 MySQL 计数行缺失或损坏，先暂停该对象消费者并等待在途事务结束，按当前关系表修复计数，再恢复消费，避免事件因计数异常一直无法落库。
2. 暂停该对象的日志清理，扫描存活 Stream（包括已 ACK 的事件），按用户取最大事件版本，联合可信的 Redis 用户版本形成恢复水位；补投事件，直到 MySQL 每个用户的版本达到水位。版本相等时还须核对状态。Lua 部分失败时事件可能领先于 Hash，不能只检查 Hash 或 MQ 队列深度；已删除事件必须满足第 6 节的落库条件。
3. 从 MySQL 一致性快照读取关系，恢复所有用户版本（包括 `liked=0`）、已点赞用户集合，并按 `liked=1` 重算计数、修正计数表。
4. 使用带唯一任务标识的同槽临时 Key 分批构建，通过短 Lua 校验 fencing token、Key 类型和快照完整性后替换状态；空集合须删除旧 Key。保持 RECOVERING 直到全部替换成功，最后设置 READY、恢复写入和清理。版本不得归零，未被数据库版本覆盖的事件不得销毁。

Redis 重启、故障切换或部分 Key 损坏后，应先隔离受影响分片，再完成事件核对与重建，避免从较旧副本继续生成重复版本。若事件及最新状态均已丢失，只能恢复可验证的持久化状态并明确丢失范围，不能声称自动恢复全部成功请求。

若 Stream 已丢失或回退，必须先暂停相关 Relay 并等待在途发送结束，再核对 MQ 各相关队列的发送水位、消费完成情况及重试/死信；不能用消费位点越过消息代替业务提交确认。无法证明旧事件已落库或被新版本覆盖时保持停写，不自动发布 MySQL 快照。fencing token 的领取与发布校验使用同一协调机制，不能仅依赖一个可能过期的锁。

定期核对 MySQL 的 `like_count` 与 `COUNT(user_like WHERE liked=1)`，并抽样检查 Redis 集合、计数、版本。Redis 与 MySQL 的差异要结合消费水位判断；修复时暂停对象写入、排空事件后再重算，不直接覆盖正在变化的计数。

## 10. 并发、容量与验收

同一对象的 Redis 写入在同槽串行；MySQL 同一计数行也存在锁竞争。异步只能削峰，不能减少总写入量，也不能靠增加消费者无限提升单对象吞吐。

初版先采用短 Lua、用户/对象限流、有界消费者并发和积压背压。按实际压测确定容量；确有热点瓶颈再按用户分桶，将关系、计数、版本、Stream 一起分桶，查询汇总计数，同时改造 MySQL 分桶计数与恢复流程。

| 验收场景 | 预期结果 |
| --- | --- |
| 同一用户并发重复点赞 | 只变更一次、一个版本、一个事件 |
| 点赞、取消后再次收到 LIKE | 按当前未点赞状态执行点赞，版本递增 |
| 请求超时 | 返回结果未知，查询当前状态，不推断原请求未执行 |
| MQ 重复或乱序 | 最终状态正确，计数不重复增减 |
| 同版本不同状态 | 告警、隔离对象，不静默忽略 |
| Lua 在 XADD 后、更新版本前失败 | 按 Stream 事件水位落库后重建 |
| MQ 已确认但消费前消息过期 | Stream 保留未落库事件，可按原事件补投 |
| Relay 在发送成功后宕机 | pending 被接管，重复消息安全落库 |
| 数据库事务失败或死锁 | 状态与计数同时回滚，消息重试 |
| MQ 长时间不可用、死信堆积 | 告警并背压，失败事件保留且可重放 |
| Redis 缺 Key、切主或恢复旧备份 | 隔离后重建，版本不从零重新开始 |
| Lua 写入阶段异常、计数上下溢 | 停止该对象服务，核对事件并修复 |

监控 API 延迟与错误率、Stream 未投递量及最老 pending 时长、MQ 消费延迟与死信数、MySQL 锁等待、Redis 内存/AOF/复制状态，以及对账差异。
