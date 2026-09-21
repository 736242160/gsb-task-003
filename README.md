# 零依赖异步服务网格流量治理与熔断降级仿真内核

一个仅使用 **Python 官方标准库** 的轻量级 Service Mesh 仿真内核，内置开箱即用的
**纯原生 HTML/CSS/JavaScript + Canvas** 可视化控制台。无任何第三方 pip 包，
无任何 CDN 资源。

## 快速开始

```bash
# 运行全部单元测试，测试通过后自动启动控制台并打开浏览器
python main.py

# 仅启动服务
python main.py --serve

# 仅运行测试
python main.py --test

# 自定义端口 / 不自动开浏览器
python main.py --serve --port 9000 --no-browser
```

启动后访问 `http://127.0.0.1:8200/`。

## 控制台能做什么

- **Canvas 实时拓扑**：`Gateway → order(3 节点) → pay(3 节点) → downstream`，
  链路上有随吞吐流动的脉冲动画；节点边框颜色即断路器状态
  （绿=CLOSED / 红=OPEN / 黄=HALF_OPEN / 灰=离线），并展示健康度、EWMA 延迟、
  动态权重、在途请求数与实时 rps。
- **一键演示①**：基线 20 rps → 抬到 60 rps → 140 并发突发，观察网关令牌桶削峰、
  订单层按动态权重平滑分流，随后回落。
- **一键演示②**：向 `pay-1` 注入 +650ms 延迟（超时阈值 400ms），
  连续超时后断路器立刻 **OPEN 切除流量并快速失败**，链路实时重定向到健康节点；
  冷却结束自动进入 **HALF_OPEN** 放行探测，探测失败则重置冷却重新 OPEN；
  故障清除后探测成功恢复 **CLOSED**，节点按健康度缓慢回灌流量完成自愈。
- **手动控制**：背景流量滑杆、并发突发按钮、延迟/错误率注入、节点上下线、一键重置。
- **时序治理日志**：长轮询实时滚动，按级别着色（熔断、快速失败、削峰、故障转移等）。

## 内核设计

| 模块 | 职责 |
| --- | --- |
| `service_mesh/breaker.py` | 断路器三态状态机（CLOSED/OPEN/HALF_OPEN），滑动窗口错误率 + 连续失败双触发条件，半开探测配额，可注入时钟 |
| `service_mesh/events.py` | 有界线程安全事件日志，基于 `Condition` 的长轮询通知 |
| `service_mesh/kernel.py` | asyncio 仿真内核：SWRR 动态权重负载均衡、令牌桶限流、舱壁隔离、熔断集成、一次故障转移重试、背景流量发生器、JSON 快照 |
| `service_mesh/demo.py` | 两个脚本化演示场景的时间线编排器（后台线程 + epoch 防重入） |
| `service_mesh/server.py` | 基于 `http.server.ThreadingHTTPServer` 的静态资源 + JSON API 服务 |
| `web/` | 纯原生前端（Canvas 拓扑、fetch 轮询/长轮询） |
| `tests/` | 22 个 `unittest` 用例：状态机、SWRR、限流、熔断、半开自愈、并发压力、HTTP API |

### 负载均衡（SWRR + 动态权重）

Nginx 风格的平滑加权轮询，每个周期累加 `current_weight`、选出最大者后减去总权重，
保证流量以“平滑、无突刺”的方式按比例分发。动态权重：

```
effective_weight = base_weight × clamp(base_latency / EWMA_latency, 0.2~2.0) × health/100
```

慢节点与不健康节点被自然降权；半开自愈的节点只获得很小比例的流量。

### 断路器状态机

- **CLOSED**：记录 10 秒滑动窗口内结果；错误率 ≥ 50%（≥4 样本）或连续失败 3 次
  立即跳闸。
- **OPEN**：6 秒冷却内所有请求 **快速失败**，完全不触碰节点，防止级联雪崩。
- **HALF_OPEN**：冷却结束后仅放行 2 个并发探测；连续 2 次成功 → CLOSED；
  任意一次失败 → 立刻 OPEN 并重新计时冷却。

### 线程安全

内核所有共享状态由单个可重入 `RLock` 保护（断路器各有独立锁，事件日志独立锁，
锁获取顺序固定为 kernel → breaker / events，无环路）；阻塞点全部是 `asyncio`
协作式等待。Web 线程通过 `run_coroutine_threadsafe` 提交协程。
`tests/test_concurrency.py` 用 6 个请求线程 + 故障切换线程 + 快照线程持续混跑
6 秒，验证无死锁、无异常、快照始终可序列化。

## HTTP API

```
GET  /api/state                内核 + 演示状态快照
GET  /api/events?after=<seq>   长轮询增量治理事件
POST /api/action               {"action": "demo|burst|traffic|inject_latency|..."}
```
