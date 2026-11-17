# TCP 故障代理 / 采样包上传卡顿复现平台

设备接入测试场景：复现 32 MiB 采样包上传卡顿，并判断超时发生在**上传阶段**还是
**回执阶段**。本项目用 **FastAPI + asyncio 单进程**实现一个 TCP 故障代理，固定转发到
本地接收端，可按连接、按方向在累计转发 N 字节后**暂停**，并支持查询、恢复、强断。

## 架构

```
 sender (FastAPI)  ──TCP:9000──▶  proxy (asyncio 转发 + FastAPI 控制面:8080)  ──▶  receiver (asyncio:9100)
   /docs 发起上传                    /docs 预设暂停/查询/恢复/强断                    收 EOF 才出回执(字节数+SHA-256)
   32MiB 后 write_eof                每方向独立 pump 任务                            回执 JSON 后反向半关闭
   读回执直到 EOF                    回执查询 HTTP:9101
```

三个组件各一个容器（`docker compose up`），也可在本机直接用脚本起三个进程。

| 组件 | 端口 | 说明 |
| --- | --- | --- |
| receiver | TCP 9100 / HTTP 9101 | 收满到 EOF 才计算并返回 `{bytes, sha256, ...}` 回执；HTTP 查最近回执 |
| proxy | TCP 9000 / HTTP 8080 | 故障转发入口；8080 为控制 API 与 `/docs` |
| sender | HTTP 8081 | `/docs` 发起上传、查看 run 与回执 |

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 控制文档
open http://127.0.0.1:8080/docs    # 故障代理：预设/连接/恢复/强断
open http://127.0.0.1:8081/docs    # 发送器：发起上传、查看回执
# receiver 回执
curl http://127.0.0.1:9101/receipts
```

Compose 为三个容器都配置了 HTTP 健康检查，并按健康状态串行启动
（receiver → proxy → sender），`docker compose ps` 显示 healthy 后再操作。
容器内端口固定为 8080（sender/proxy 控制面）；proxy 的 TCP 入口为 9000。

无 Docker 时（本机已有 Python 3.11）：

```bash
scripts/run-local.sh        # 建 venv、装依赖、起三个进程（日志在 /tmp/fp-logs）
scripts/stop-local.sh       # 停止
python -I tests/smoke_test.py                    # 端到端冒烟（37 项检查）
python -I tests/test_connect_lifecycle.py        # 建连/强断生命周期（68 项检查）
python -I tests/integration_stuck_target.py      # 真实挂起建连（需 192.0.2.1 不可路由）
```

## 建连期间的连接管理（本次修复重点）

设备接入测试员会用“接收端建连卡住”（例如接收端 accept 队列打满、或目标地址被黑洞）
复现客户端连上了代理、却在代理里找不到连接、也无法强断的问题。当前行为：

- **客户端接入即可见**：代理 `accept` 到客户端连接后立即登记，`GET /connections`
  立刻能查到 `id`、`state="connecting"` 和 `connect` 子对象（目标、开始时间、超时）。
  建连完成前 `GET /connections/{id}`、`/pause`、`/resume` 都可用。
- **建连中可强断**：`POST /connections/{id}/force-close` 会取消挂起的目标建连、
  abort 客户端 socket，**清理完成后才返回**；返回体是已关闭记录
  （`closed=true`、`close_reason`）。重复强断同一 id 返回 409 而不是挂起。
- **强断撞上建连刚成功**：若目标套接字在取消窗口内建好，清理会取回该连接结果并
  直接关闭它；不会启动两个转发 pump，也不会在代理侧留下活动目标连接。
  `connect.state` 记为 `aborted`（不是 `connected`）。
- **建连失败有据可查**：目标拒绝/超时后客户端被关闭，`/connections` 保留已关闭记录，
  `error` 字段带原因（如 `connect target receiver:9100 failed: ConnectionRefusedError(...)`，
  或 `TimeoutError()`），`connect.state="failed"`。
- **互不阻塞**：挂起的建连不占用控制 API，也不影响其他连接的建连/转发；每个连接
  建连使用独立任务与独立超时（`PROXY_CONNECT_TIMEOUT`，默认 5s；可设 0 关闭超时）。
- `GET /health` 与 `GET /connections` 均返回 `connecting_count`，便于确认没有建连悬挂。

复现“建连中强断”的最简方式（无需 Docker）：

```bash
# 目标指向不可路由地址，建连会挂起；加大建连超时以便观察
TARGET_HOST=192.0.2.1 TARGET_PORT=9100 PROXY_CONNECT_TIMEOUT=30 \
  python -m uvicorn proxy.app:app --host 127.0.0.1 --port 8080 &
nc 127.0.0.1 9000 &                     # 客户端接入（保持不主动断开）
curl -s http://127.0.0.1:8080/connections | python -m json.tool
#  -> state=connecting，id 可查
curl -s -X POST http://127.0.0.1:8080/connections/<id>/force-close | python -m json.tool
#  -> 立即返回 closed=true, close_reason="force-closed by control API"
```

## 复现卡顿并定位超时阶段（标准操作流）

1. 在代理 `/docs` 调 `PUT /preset`，设 `c2r_pause_after=1048576`（上传 1 MiB 后暂停）。
2. 在发送器 `/docs` 调 `POST /send`（默认 32 MiB），拿到 `run_id`。
3. `GET /connections` 看到连接 `c2r` 方向 `paused=true`、`forwarded_bytes=1048576`、
   `fin` 未送达；发送器 run 停在 `connected`（drain 被 TCP 背压阻塞）——**说明超时在上传阶段**。
4. `POST /connections/{id}/resume`（`direction=c2r`），积压与 FIN 继续，接收端出回执，
   run 变 `done`，`hash_match=true`。
5. 要定位**回执阶段**超时：`PUT /preset` 设 `r2c_pause_after=16`，此时上传正常完成
   （`c2r.half_closed_delivered=true`），run 停在 `waiting-receipt`，恢复 `r2c` 后才收到回执。
6. `POST /connections/{id}/force-close` 可随时强断，立即打断暂停与阻塞写。

## 控制 API（proxy:8080）

| 方法/路径 | 作用 |
| --- | --- |
| `GET /health` | 健康状态、转发目标、建连超时、活动/建连中连接数、每方向用户态缓冲上限（256 KiB） |
| `GET /preset` / `PUT /preset` | 查看/预设**新连接**每方向暂停阈值（`null` 取消） |
| `GET /connections` | 全部连接（活动在前、已关闭在后），含 `connecting_count`、建连状态与每方向字节/FIN 状态 |
| `GET /connections/{id}` | 单连接详情；活动连接实时快照，已清理记录也可查（404 仅当从未存在） |
| `POST /connections/{id}/pause` | 手动暂停某方向（阈值之外的测试手段，建连中亦可用） |
| `POST /connections/{id}/resume` | 恢复指定方向；不传 direction 则恢复全部暂停方向 |
| `POST /connections/{id}/force-close` | 强断：建连中取消建连并关闭客户端，已连通则 abort 两端、取消 pump；清理完成才返回；对已关闭 id 返回 409 |

连接字段：`state`（`connecting`/`connected`/`closing`/`closed`）、`connect`（`state`、
`target`、`started_at`、`connected_at`、`duration_seconds`、`timeout_seconds`、`error`）、
`close_reason`、`error`。每方向字段：`forwarded_bytes`（已转发）、`pending_bytes`
（暂停时暂扣的积压）、`paused`、`fin_pending`、`half_closed_delivered`、`eof_seen`、
`peer_closed` 等。

## 关键正确性设计与保证

- **恰好停在 N 字节（跨读取块）**：每次 `read(64KiB)` 后按阈值切分，`head` 转发、
  `tail` 作为该方向唯一的用户态积压（`pending`）；暂停期间不再读 socket。
- **积压与 FIN 不越过暂停**：暂停时不做任何读和 FIN 投递；读到 EOF 也只标记
  `eof_seen/fin_pending`，等恢复后先把积压发完再 `write_eof()`。
- **恢复后不丢、不重、不乱序**：恢复后先 flush `pending` 再继续读，字节顺序唯一；
  测试以双端 SHA-256 与字节数比对验证。
- **背压、无整包缓存、不落盘**：暂停即停读 → 内核接收缓冲填满 → TCP 流控反压到发送端，
  发送端阻塞在 `drain()`；每方向用户态数据 = StreamReader 缓冲（上限 256 KiB）+
  至多一个切分尾块，全程不写磁盘。
- **FIN 半关闭语义**：FIN 等本方向积压清空后才向对端半关闭；反向 pump 是独立任务，
  回执方向不受影响，可一直读到 EOF。
- **强断语义**：`transport.abort()` 让阻塞的 `drain()`/`read()` 立即抛错，两个 pump
  被取消并退出，supervisor 关闭两端 socket、连接从活动列表移除后 API 才报完成；
  其他连接与控制 API 不受影响（冒烟测试覆盖）。
- **建连状态机**：连接先以 `connecting` 登记，再异步建目标连接；建连结果通过
  “无 await 的 `attach_remote` 状态检查 + 起 pump” 原子交接。强断先同步置
  `closing`，因此无论取消落在 DNS/握手/传输层哪个 await 点，建连任务都不会再启动转发；
  取消窗口内已建成的目标套接字由清理过程取出并关闭（实测 400 次不同取消时点 +
  FD 计数无泄漏）。
- **强断的完成边界**：`force-close` 等待建连任务、两个 pump、连接清理全部结束后再返回；
  清理用每连接一次性锁保证并发强断/关停只执行一次，重复请求不会挂起。

## 测试

- `tests/smoke_test.py`（纯标准库）覆盖转发正确性：基线哈希一致、自动暂停精确停 N、
  积压/FIN 暂扣、奇数 N 跨块、手动暂停/恢复、回执方向暂停、强断与隔离性、
  背压下用户态积压 ≤ 256 KiB、32 MiB 级载荷恢复后哈希一致。
- `tests/test_connect_lifecycle.py`（自包含，进程内起控制 API + 桩目标）覆盖建连期
  连接管理：接入即可见、建连中控制 API 可用、建连中强断（取消 + 关客户端 + 返回已
  关闭记录）、强断与“目标刚连通”竞态（新目标套接字被关闭、不启动 pump，含白盒断言）、
  目标超时/拒绝保留原因、其他连接不受挂起建连影响，以及正常路径回归。
- `tests/integration_stuck_target.py`（可选，真实网络挂起）用不可路由的
  `192.0.2.1` 让目标建连真实卡住，分别验证强断路径与建连超时路径。

可用环境变量 `PROXY_API/SENDER_API/RECEIVER_API/PROXY_TCP` 让冒烟测试指向
Compose 暴露的端口直接对容器跑。

```bash
python -I tests/smoke_test.py                 # 37/37 checks passed
python -I tests/test_connect_lifecycle.py     # 68/68 checks passed
STUCK_MODE=force   python -I tests/integration_stuck_target.py   # OK
STUCK_MODE=timeout python -I tests/integration_stuck_target.py   # OK
```

## 实测结果（本环境，Python 3.11.2 / loopback）

环境无 Docker 守护进程，故用等价的三进程本地部署验证（Compose 文件已就绪，逻辑与
容器部署一致，仅主机名改为 127.0.0.1）。连续多轮结果：

- 冒烟套件 **37/37 通过**，并连跑 8 轮无回归；覆盖下列断言。
- 精确停点：阈值 N=300000、100007（奇数，跨 64KiB 读块）、1048576、4194304、0
  均实测 `forwarded_bytes == N`，暂停期间计数冻结、FIN 不送达。
- 背压：16 MiB 载荷在 N=300000 暂停后，发送器实际卡在 `drain()`（仅发出约 3.75 MiB），
  代理 `pending_bytes` 峰值约 62 KiB，全程 **≤ 256 KiB**，接收端无回执。
- 恢复完整性：恢复后字节数与双端 SHA-256 全部一致（含 32 MiB 完整采样包，
  实测 33554432 字节、约 3–7 秒、`hash_match=true`、接收端 `eof_seen=true`）。
- 回执阶段：`r2c_pause_after=16` 时上传照常半关闭完成，run 停在 `waiting-receipt`，
  恢复 r2c 后才收到回执——可据此区分超时在上传还是回执阶段。
- 强断：暂停中的连接 `force-close` 约 20–40 ms 返回，发送端收到
  `ConnectionResetError`，连接从活动列表移除，控制 API 与后续新连接不受影响。
- N=0 边界：立即暂停、暂扣全部字节与 FIN，恢复后 8 字节测试载荷与回执完整。

### 建连期修复的实测（同一环境）

- `tests/test_connect_lifecycle.py` **68/68 通过**：建连挂起时连接在 2 ms 内可查、
  控制 API 响应约 2–3 ms；建连中 `force-close` 约 2 ms 返回，客户端立即 EOF，
  挂起建连被取消；三次“目标恰好连通”竞态中目标 socket 全部关闭、无 pump 启动；
  目标超时 1.5s / 拒绝均留下带原因的关闭记录；其他连接 27 ms 内建连成功；
  同一挂起连接 3 个并发强断请求全部返回（200）、仅留一条关闭记录且未启动转发。
- `tests/integration_stuck_target.py`（真实不可路由目标 192.0.2.1）两种模式均 OK：
  强断模式建连中 2 ms 返回并关闭客户端；超时模式 2s 后留下
  `connect target 192.0.2.1:9100 failed: TimeoutError()` 记录。
- 取消时点扫描：400 次不同取消延迟下客户端/目标端 FD 计数无增长（delta ≤ 0）；
  建连成功后强断 300 次亦无 FD 泄漏，活动连接数归零。
- 以上全部在无 Docker 的本机三进程部署上复测：冒烟 **37/37**、生命周期 **68/68**
  （连跑 3 轮与 asyncio dev 模式均稳定），proxy/receiver/sender 日志无异常；
  Compose 已加健康检查与按健康串行启动。

> 说明：发送器对回执读取带 `SENDER_RECEIPT_TIMEOUT`（默认 120s）超时保护，
> 回执阶段长期无 EOF 时 run 置 `failed` 并注明 receipt-phase stall，而非永久挂起。

## 目录

```bash
proxy/app.py       故障代理（asyncio 双向 pump + FastAPI 控制面）
sender/app.py      发送器（确定性 32MiB 采样流、半关闭、读回执，FastAPI /docs）
receiver/app.py    接收端（读到 EOF 算字节数/SHA-256，回执 + 极简 HTTP 查询）
tests/smoke_test.py                 端到端冒烟（转发/暂停/背压）
tests/test_connect_lifecycle.py     建连期连接管理与强断（自包含）
tests/integration_stuck_target.py   真实挂起建连（可选，需不可路由目标）
scripts/           本机启停脚本
Dockerfile docker-compose.yml requirements.txt
```
