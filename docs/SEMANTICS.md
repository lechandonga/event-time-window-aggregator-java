# 窗口与水位线语义

本服务是一个完全本地运行的事件时间（event-time）滚动窗口聚合服务，不依赖任何外部中间件。

## 基本定义

- 窗口大小：`etwa.window-size-ms`（默认 10s），滚动窗口按 epoch 毫秒对齐，
  事件 `eventTime=t` 归属窗口起点 `floor(t / windowSize) * windowSize`。
- 容忍延迟：`etwa.allowed-lateness-ms`（默认 5s）。
- 水位线（watermark）：`WM = max(已观察 eventTime) - allowedLatenessMs`，
  在消费端单线程内随事件单调推进。

## 窗口关闭与可见性

- 当且仅当 `WM >= 窗口结束时间 windowEnd` 时窗口关闭；关闭动作在处理"推进水位线的那条事件"时同步发生。
- 窗口一旦关闭：
  - 聚合结果立刻写出到 sink（`data/results.jsonl`）并在 `GET /results` 可见；
  - 结果**不可变**，之后任何事件都不会再修改它。
- 未关闭窗口的中间聚合对外完全不可见（`GET /results` 只返回已关闭窗口）。

## 乱序与迟到

- 在窗口关闭前到达（即 `eventTime` 所属窗口的 `windowEnd > WM`）的乱序事件一律正常计入，
  最终结果只由事件集合决定，与到达顺序无关——并发乱序接入与串行接入结果一致。
- 窗口已关闭后到达、且事件属于该已关闭窗口的事件为"过度迟到"：
  - 不改动任何已关闭窗口；
  - 进入死信路径，原因码 `TOO_LATE`，detail 中包含关闭窗口区间与当前水位线；
  - 绝不静默丢弃。

## 去重

- 每个事件必须带全局唯一 `id`。
- 每个窗口累加器保留贡献过它的全部事件 id；同一 id 重复投递（客户端重试、崩溃重放等）
  在窗口内只计数一次，并计入 `duplicate` 指标。

# 一致性保证

## 先落日志后消费

`POST /events` 的处理顺序是：

1. 事件原文（JSON 字节）追加到本地 WAL（`data/events.wal.log`），
   每条记录形如 `<offset>:<base64(json)>`，offset 从 0 严格递增；
2. 追加后执行 `fsync`，成功才向调用方返回 `202 ACCEPTED`（或背压时的 503，见下节）；
3. 记录进入有界内存队列，由唯一的消费者线程处理。

因此被确认的事件在进程被杀后必然仍可从 WAL 恢复；未确认的事件不存在"已计入"的可能。

## 位点与状态的原子检查点

- 消费每处理完一条 WAL 记录，就把「消费位点 + 全部聚合状态 + 当前水位线」
  写入**同一个**检查点文件 `data/checkpoint.json`（temp 文件 fsync 后原子 rename，目录再 fsync）。
- 位点和状态因此不可能出现"位点提交了但状态没提交"或相反的中间态。
- 检查点文件首行是 `{"sha256":"..."}` 信封，校验和覆盖其后状态 JSON 的原始字节，
  可检测撕裂写、截断和篡改。
- 检查点还保存最后处理记录的 WAL 原文 SHA-256，启动时用该记录重新计算摘要交叉核对，
  确保"位点指向的记录"和"状态"属于同一份历史。

## 启动恢复（明确拒绝未知中间态）

启动时依次校验，任一不满足都抛出 `CheckpointCorruptedException` 拒绝启动，
而不是从猜测的中间状态继续：

1. WAL 中有记录但检查点缺失 → 拒绝（只有 WAL 与检查点都不存在时才按全新状态初始化）；
2. 检查点信封/JSON 无法解析 → 拒绝；
3. 载荷 SHA-256 与信封不一致 → 拒绝（`checksum mismatch`）；
4. 检查点版本不支持、窗口参数（windowSize/allowedLateness）与当前配置不一致 → 拒绝；
5. `lastConsumedOffset` 超出 WAL 长度或在 WAL 中不存在 → 拒绝；
6. offset 对应记录的摘要与检查点保存的摘要不一致（状态与位点分叉）→ 拒绝。

校验通过后，将 `[lastConsumedOffset+1, WAL末尾)` 的记录在消费者线程启动前**同步重放**，
已处理区间不会重复处理；重复事件由窗口内 id 集合再次兜底，保证重启后不重不漏。

# 背压与死信策略

## 有界积压与显式背压

- 接入线程与消费线程之间是容量为 `etwa.queue-capacity`（默认 1000）的有界队列。
- 队列满时采用**非阻塞 offer**：事件已经安全落在 WAL 中，但立即返回
  `503 QUEUE_FULL`，不会无限拖住 HTTP 线程；调用方可稍后重试（WAL 保证不丢，
  重启恢复时同样会被消费）。
- `rejectedQueueFull` 计数、`queueSize`、`queueCapacity` 均可通过 `GET /metrics` 观测。

## 写出重试与死信

- 关闭窗口的结果经 `RetryingSink` 写出：失败按固定退避
  （`etwa.sink-backoff-ms`）最多重试 `etwa.sink-max-attempts` 次（含首次）。
- 重试仍失败的结果进入死信（`data/dead-letters.jsonl`，并可通过 HTTP 查询），
  原因码 `SINK_RETRY_EXHAUSTED`，detail 包含尝试次数、错误信息和涉及的事件 id。
  聚合内存状态不受 sink 失败影响，窗口结果仍可通过 `GET /results` 查询。
- 死信原因码共三种，彼此可区分：
  - `TOO_LATE`：超出容忍范围的迟到事件；
  - `MALFORMED`：无法解析/校验的事件原文（消费跳过并继续推进位点）；
  - `SINK_RETRY_EXHAUSTED`：结果写出重试耗尽。

# HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/events` | 提交事件 JSON：`{id,key,eventTime,value}`；202 已落日志并入队，503 队列已满 |
| GET | `/results?windowStart=&key=` | 只返回已关闭窗口结果，可按窗口起点/key 过滤 |
| GET | `/dead-letters?reason=` | 查询死信，可按原因码过滤 |
| GET | `/metrics` | 接入/消费/重复/迟到/重试/死信/窗口关闭/队列等计数 |
| GET | `/health` | 处理管线运行状态、最后消费位点与水位线 |

# 本地验证

## 运行单元测试

```bash
mvn test
```

- 共 18 个用例，原有 `ApplicationTests.contentLoad` 保持通过；
- 不依赖外部服务、不使用固定端口（HTTP 集成测试使用 `RANDOM_PORT`，
  数据目录为 `@TempDir`/`${java.io.tmpdir}` 下的临时目录）；
- 控制台中的 `SCENARIO-LEDGER` 日志按业务场景打印覆盖台账，形如：
  `================ 业务场景覆盖: 死信路径：迟到/坏消息/写出失败 ================`，
  每个用例一行 `[PASS]/[FAIL]/[SKIP]` 及中文用例名，业务结束时输出
  `业务[...]用例汇总: 共 N 个, PASS=.., FAIL=.., SKIP=..`。

覆盖的业务场景：

- 乱序事件的事件时间窗口聚合（含容忍范围内迟到、未关闭窗口不可见、乱序=顺序结果）；
- 重复投递幂等去重（HTTP 重投、重启重放）；
- 崩溃重启恢复（未消费 WAL 重放、多次重启幂等）；
- 检查点完整性（缺失/篡改校验和/参数不一致均拒绝恢复）；
- 死信路径（TOO_LATE / MALFORMED / SINK_RETRY_EXHAUSTED）；
- 有界积压与背压（队列满立即 503、拒绝数可观测）；
- HTTP 端到端（8 线程并发接入结果等于唯一 id 集合、接口契约）。

## 本地启动并手工验证

```bash
mvn spring-boot:run
```

另开终端（默认窗口 10s、容忍 5s）：

```bash
curl -s -XPOST localhost:8080/events -H 'Content-Type: application/json' \
  -d '{"id":"e1","key":"k1","eventTime":11000,"value":4}'
curl -s -XPOST localhost:8080/events -H 'Content-Type: application/json' \
  -d '{"id":"e2","key":"k1","eventTime":19000,"value":6}'
# 此时窗口未关闭，/results 为空
curl -s 'localhost:8080/results'
# 推进水位线关闭 [10000,20000)
curl -s -XPOST localhost:8080/events -H 'Content-Type: application/json' \
  -d '{"id":"e3","key":"k1","eventTime":30000,"value":0}'
curl -s 'localhost:8080/results?windowStart=10000&key=k1'
# 过度迟到事件 -> TOO_LATE 死信
curl -s -XPOST localhost:8080/events -H 'Content-Type: application/json' \
  -d '{"id":"late","key":"k1","eventTime":10500,"value":99}'
curl -s 'localhost:8080/dead-letters?reason=TOO_LATE'
curl -s 'localhost:8080/metrics'
```

数据文件都在 `etwa.data-dir`（默认 `./data`）：
`events.wal.log`、`checkpoint.json`、`results.jsonl`、`dead-letters.jsonl`。
需要从空白状态重启时，停止进程后显式清空该目录即可。
