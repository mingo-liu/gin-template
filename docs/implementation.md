# Likeflow 使用与运维

实现遵循 [设计方案](like-system-design.md)，目录仍按 routes → handler → service → repository 分层。`internal/worker` 负责 Relay、核对补投与恢复，`internal/queue` 封装官方 RocketMQ Go v2 客户端，`web` 内嵌无构建依赖的页面。

## 本地启动

需要 Go 1.26+ 和 Docker Compose。

```sh
cp .env.example .env
# 编辑 .env，设置至少 32 字节随机 SESSION_SECRET。
set -a
source .env
set +a
make infra
# 等待 MySQL 健康、Broker 启动完成后创建主题：
make topic
make run
```

访问 http://localhost:8080 。页面支持三位演示用户、点赞/取消、刷新、操作记录和 MySQL 同步进度。只有 `DEMO_MODE=true` 才开放演示登录；这是公开测试身份入口，部署到实际业务前关闭并接入登录服务。API 从 HttpOnly、SameSite=Strict 的签名 Cookie 或 Bearer Token 获取用户身份，不能通过 Body/用户 ID 请求头冒充用户。Token 格式及签发逻辑见 `internal/middleware`；密钥更换会使现有会话失效。

Go 程序读取环境变量，不自动读取 `.env`。Compose 首次初始化数据卷时按文件名顺序执行 `migrations/*.sql`；已有数据库需要手工执行新增迁移。SQL 显式设置连接字符集为 utf8mb4，避免客户端默认 latin1 导致中文乱码。已有乱码可执行 `002_repair_demo_encoding.sql`，该迁移只修复与已知错误编码匹配的演示文字，可重复执行，不修改点赞状态或自定义内容。

```sh
docker compose -f deploy/docker-compose.yml exec -T mysql \
  sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" mysql --default-character-set=utf8mb4 -u"$MYSQL_USER" "$MYSQL_DATABASE"' \
  < migrations/002_repair_demo_encoding.sql
```

三篇演示文章都是公开可访问对象，类型 1/2/3 均受 API 和事件校验支持，新增对象时必须同时创建目录及准确的计数行。已有关系的对象不能按新对象初始化。

默认镜像是 MySQL 8.4、Redis 7.4、RocketMQ 5.3.2。可通过 `MYSQL_IMAGE`、`REDIS_IMAGE`、`MQ_IMAGE`、`MQ_PLATFORM` 覆盖；例如 Apple Silicon 原生镜像可指定 `MQ_PLATFORM=linux/arm64`。默认 Broker 地址供宿主机 Go 程序使用；容器内运行 Go 服务须调整 `brokerIP1` 为所有客户端均可达的地址。Compose 数据卷持久化，端口仅绑定本机；Broker 在本地开发编排内以 root 运行，避免卷目录权限导致启动失败。

## API

```sh
curl -c /tmp/likeflow-cookie -H 'Content-Type: application/json' \
  -d '{"user_id":"1001"}' localhost:8080/api/v1/session
curl -b /tmp/likeflow-cookie -X PUT -H 'Content-Type: application/json' \
  -d '{"action":"LIKE"}' localhost:8080/api/v1/targets/1/2001/like
curl -b /tmp/likeflow-cookie localhost:8080/api/v1/targets/1/2001/like
# 取消时将 action 改为 UNLIKE
curl -b /tmp/likeflow-cookie localhost:8080/api/v1/targets/1/2001/sync
```

响应包括 `liked/count/version/changed/code`。ID、计数和版本均为十进制字符串。重复请求成功返回 `ALREADY_LIKED` 或 `NOT_LIKED`，`changed=false`。`sync` 单独返回 MySQL 已落库状态，不用落后结果覆盖 Redis。批量查询是设计中的可选功能，本版未增加。

错误状态：参数错误 400、未登录 401、跨站写入 403、对象不存在 404、限流 429、依赖异常或恢复中 503。PUT 的 503 保守返回 `RESULT_UNKNOWN`；超时不自动重发旧动作，也不保证客户端意图顺序。页面同一时刻串行提交操作并禁用身份切换，异常后查询状态并提示请求可能仍在处理。

## 一致性与运行边界

- Lua 校验类型、整数上限、计数与集合一致性；读写均在一个脚本内。写入前设置 RECOVERING，全部成功才 READY。脚本内部校验实际 Redis `run_id`，关闭 INFO 检查和执行之间重连到新节点的窗口。
- Redis 使用独立主节点、AOF、noeviction，不设 TTL。服务检查主节点角色、AOF 写状态、淘汰策略。MySQL 对象目录持久化绑定 Redis 身份，重启和切主后拒绝继续在线服务，不能自动清零版本。
- Relay 周期读取持久对象目录并扫描 Redis Stream 补漏，孤立日志告警。各对象单独 XREADGROUP，固定组 `like-relay`；仅 SendOK 才 XACK；30 秒 XAUTOCLAIM 接管，超过单次 5 秒发送期限。单个 worker 串行有界转发，消费者并发为 4。
- XACK 后仍保留日志。周期遍历全部存活日志，与主库关系版本核对，落库才通过同槽 Lua XACK+XDEL；同版本须状态相同。未落库超过一分钟按原事件身份补投，覆盖 MQ 过期和死信情况；不存在 MAXLEN 裁剪。
- 每对象未清理日志达到 `MAX_BACKLOG`（默认 10000）停止新变更，查询和无变化重试仍可返回。用户每秒 30、对象每秒 200 请求限制为**进程内**保护；多 API 副本部署需入口统一限流。本版是单 Redis 主节点部署，未提供 Cluster/Sentinel 自动故障切换适配。
- MySQL 锁顺序固定为关系、计数；更新、缺行检查和边界校验均在事务内。重复、乱序及跨版本消息允许；相同版本不同状态隔离对象，失败消息返回重试，16 次后由 Broker 转入死信。
- 新对象先持久化初始化标记，再初始化 Redis，避免进程崩溃后再次自动补零。初始化途中失败必须人工检查，不能撤销标记后盲目重试。

Compose 是单 Redis、单 Broker 的本地验证配置，AOF everysec 和 SYNC_FLUSH 仍不保证灾难后不丢数据。实际部署须配置 Redis 主从及故障入口停写、Broker 复制和持久化确认策略、磁盘及内存容量告警、主库连接、ACL 和私有网络。若成功响应必须严格不丢，按设计改用 MySQL 事务 Outbox。

## 恢复与对账

发现 RECOVERING、重启/切主、版本冲突时先隔离故障节点及入口，禁止任何其他程序修改相关 Key。程序不会自动将 MySQL 旧状态覆盖 Redis。

1. 核查 Stream 连续性、所有相关 MQ 队列、重试、死信和在途发送；不能把消息位点当成提交证据。如果 Stream 丢失或回退，先暂停所有相关 Relay 并等待在途发送结束，核实旧事件已落库或被新版本覆盖。无法证明时保持停写并恢复日志备份，不能强制发布快照。`--verified-log` 是操作员对上述核查的明确声明，不是程序自动检测出的保证。
2. 确认日志可信后执行（读取同一组环境变量）：

   ```sh
   go run ./cmd/server --recover 2001 --type 1 --verified-log
   ```

   命令以 Lua 原子冻结对象，设置不自动过期的 recovery_token，同时阻断清理。在线 Lua 与冻结脚本串行执行，所以冻结返回后没有旧在线写入能继续修改对象。存活 Stream 全量按版本落库，联合 Redis Hash 校验水位，再从 MySQL 一致性快照重建所有版本（包括取消记录）、集合和计数。临时 Key 逐条构建，短 Lua 校验 token、类型、完整性和服务身份后替换，最后 READY。未覆盖水位或冲突时保持停止状态，事件不删除。
3. 任务中断后，日志会给出 resume_token；检查故障后用 `--resume-token <旧token>` 重跑。替换 token 会阻止旧任务发布；不要同时运行多个恢复任务。失败任务的临时 Key 不设 TTL，确认旧任务已退出后按 token 精确删除其临时 Key，不能删除存活日志。Redis 整体丢失时 token 也不可信，必须先停止所有恢复任务、隔离旧主再核查备份与 MQ。
4. 计数行丢失或错误时，先停该对象写入和全部消费者，等待在途事务结束，再用事务从关系表重算并修复计数行；缺失行可按 `COUNT(liked=1)` 创建，绝不能直接补零。恢复消费后重放日志，再执行恢复命令。版本冲突先检查损坏消息和持久版本；不通过修改版本为零解决。
5. 初始化崩溃导致 Stream 尚未创建时，只有核实没有任何历史关系、状态或 MQ 消息后才能创建空消费组日志；有历史时须恢复真实日志或给出已完整持久化的证据再重建日志，不能自动补空。

Worker 每分钟从一致性快照核对 MySQL 关系计数。日志包含 API 延迟、转发错误、pending 数量和最老 ID、孤立 Stream、消费重试次数及对账差异。生产监控需采集 Stream 长度/最老 pending 时间、Redis 内存/AOF/复制、Broker 消费延迟与 `%DLQ%like-persistence`、MySQL 锁等待等指标并设置告警；修复死信后可用管理工具按原消息重放，保留 Stream 也会持续补投。监控系统和值班告警接入由部署环境提供。

## 验证

```sh
make check
make test
# 集成测试使用已迁移的独立测试库和 Redis；运行前停止服务，避免后台 Relay 干扰测试对象。
MYSQL_TEST_DSN='likeflow:likeflow@tcp(127.0.0.1:3306)/likeflow?parseTime=true' \
REDIS_TEST_ADDR=127.0.0.1:6379 go test -race ./...
# 启动服务和全部依赖后，执行真实链路冒烟：
python3 scripts/smoke.py
```

Redis 单元测试会启动临时 redis-server 实例；未安装则明确跳过。MySQL/Worker 集成测试未设置环境变量也会明确跳过，不能据此宣称验证了真实依赖。测试只删除其随机对象 ID 的数据，不清空数据库。冒烟使用演示用户 1003 与文章 2003，完成后恢复原点赞状态并等待最终落库（版本会正常递增）。

本次真实环境通过 Go race 测试、MySQL/Redis 集成测试与 RocketMQ 冒烟；另实测 Redis 重启后 API 持续返回 503、逐对象恢复后保留历史版本。镜像缓存对应 MySQL 9.7.1、Redis 8.8.0、RocketMQ 5.5.0，默认固定镜像版本未完成本机验证。前端 JS 语法检查通过；浏览器访问工具不可用，尚未完成截图和浏览器点击验证。

实现参考：[RocketMQ 官方 Go 客户端](https://github.com/apache/rocketmq-client-go)、[Redis XAUTOCLAIM](https://redis.io/docs/latest/commands/xautoclaim/)。
