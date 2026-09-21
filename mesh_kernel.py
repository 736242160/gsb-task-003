"""
mesh_kernel.py
==============

零外部依赖的异步服务网格 (Service Mesh) 流量治理与熔断降级仿真内核。

仅使用 Python 官方标准库:
    asyncio / threading / time / json / dataclasses / random / ...

核心能力:
    1. 基于动态权重的平滑加权轮询 (Smooth Weighted Round-Robin, nginx 风格),
       权重由节点延迟 (EWMA) 与健康评分共同决定, 逐拍平滑过渡。
    2. 断路器三态状态机: CLOSED -> OPEN -> HALF_OPEN -> CLOSED/OPEN,
       连续失败计数 + 滑动窗口错误率双触发, 半开状态单飞试探流量。
    3. 令牌桶突发限流 + 全局并发闸门。
    4. 全部共享状态由内核级 threading.RLock 保护; 工作协程只在拿到不可变
       快照后 await, 绝不在持锁期间挂起, 杜绝死锁与数据竞态。

线程模型:
    - 内核拥有独立的 asyncio 事件循环线程 (后台 daemon), 所有时序/仿真
      协程在该线程内运行。
    - 外部线程 (HTTP server 等) 通过 submit() 把协程投递进事件循环,
      状态读取通过线程安全的快照方法完成。
"""

from __future__ import annotations

import asyncio
import itertools
import random
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional


# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

CLOSED = "closed"
OPEN = "open"
HALF_OPEN = "half_open"

SUCCESS = "success"
TIMEOUT = "timeout"
ERROR = "error"
THROTTLED = "throttled"
REJECTED = "rejected"


@dataclass
class MeshConfig:
    """内核可调参数 (集中配置, 测试时可缩短时间尺度)。"""

    rate_limit: int = 120                 # 令牌桶速率 (请求/秒)
    burst_capacity: int = 90             # 令牌桶突发容量
    max_concurrency: int = 80            # 全局并发闸门
    failure_threshold: int = 5           # 连续失败多少次后熔断
    error_rate_threshold: float = 0.5    # 滑动窗口错误率阈值
    error_window_min: int = 8            # 错误率判定所需最小样本数
    error_window_size: int = 32          # 滑动窗口容量
    open_cooldown: float = 6.0           # OPEN 静默冷却窗口 (秒)
    half_open_max_trial: int = 1         # HALF_OPEN 同时放行的探针请求数
    half_open_success: int = 3           # HALF_OPEN 连续成功多少次后恢复
    call_timeout: float = 0.22           # 模拟调用超时时间 (秒)
    base_rps: int = 70                  # 后台常态流量 (请求/秒)
    weight_smoothing: float = 0.35       # 动态权重 EWMA 平滑系数
    weight_interval: float = 1.0         # 权重重算周期 (秒)
    health_alpha: float = 0.3            # 健康度 EWMA 平滑系数

# ---------------------------------------------------------------------------
# 断路器: 三态状态机
# ---------------------------------------------------------------------------


class CircuitBreaker:
    """单节点断路器。

    CLOSED  --(连续失败达阈值 或 错误率越限)--> OPEN
    OPEN    --(冷却窗口结束, 下一次调用)--> HALF_OPEN
    HALF_OPEN --(连续试探成功达阈值)--> CLOSED
    HALF_OPEN --(任一失败)--> OPEN (冷却窗口重置)

    自带互斥锁, 可脱离内核独立单测; 生产路径中由内核统一加锁, RLock 可重入。
    """

    def __init__(
        self,
        name: str,
        config: MeshConfig,
        clock: Callable[[], float] = time.monotonic,
        on_event: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    ) -> None:
        self.name = name
        self.cfg = config
        self._clock = clock
        self._on_event = on_event
        self.lock = threading.RLock()

        self.state = CLOSED
        self.consecutive_failures = 0
        self.consecutive_successes = 0
        self.opened_at = 0.0
        self.wake_at = 0.0
        self.active_trials = 0
        self.trials_completed = 0
        self.open_count = 0
        self._window: List[bool] = []  # True=成功 False=失败

    def _emit(self, to_state: str, reason: str, error_rate: Optional[float] = None) -> None:
        if self._on_event:
            self._on_event(
                "breaker",
                {
                    "node": self.name,
                    "state": to_state,
                    "reason": reason,
                    "error_rate": error_rate,
                },
            )

    def _trip_open(self, reason: str, error_rate: Optional[float] = None) -> None:
        now = self._clock()
        self.state = OPEN
        self.opened_at = now
        self.wake_at = now + self.cfg.open_cooldown
        self.consecutive_successes = 0
        self.active_trials = 0
        self.open_count += 1
        self._emit(OPEN, reason, error_rate)

    def _close(self, reason: str) -> None:
        self.state = CLOSED
        self.consecutive_failures = 0
        self.consecutive_successes = 0
        self.active_trials = 0
        self.trials_completed = 0
        self.wake_at = 0.0
        self._window.clear()
        self._emit(CLOSED, reason, 0.0)

    def _error_rate(self) -> Optional[float]:
        if len(self._window) < self.cfg.error_window_min:
            return None
        failures = sum(1 for ok in self._window if not ok)
        return failures / len(self._window)

    def maybe_half_open(self) -> bool:
        """冷却窗口结束后把 OPEN 推进到 HALF_OPEN。"""
        with self.lock:
            if self.state == OPEN and self._clock() >= self.wake_at:
                self.state = HALF_OPEN
                self.consecutive_successes = 0
                self.consecutive_failures = 0
                self.active_trials = 0
                self.trials_completed = 0
                self._emit(HALF_OPEN, "cooldown_elapsed")
                return True
            return False

    def can_call(self) -> bool:
        """发送请求前征询断路器; HALF_OPEN 下占用一个试探名额 (单飞探针)。"""
        with self.lock:
            if self.state == CLOSED:
                return True
            self.maybe_half_open()
            if self.state == OPEN:
                return False
            if self.state == HALF_OPEN:
                if self.active_trials < self.cfg.half_open_max_trial:
                    self.active_trials += 1
                    return True
                return False
            return False

    def release_trial(self) -> None:
        """调用未真正发生时归还试探名额。"""
        with self.lock:
            if self.active_trials > 0:
                self.active_trials -= 1

    def record_success(self) -> None:
        with self.lock:
            # OPEN 之后才返回的在途成功不影响状态机 (只能由探针驱动恢复)
            if self.state == OPEN:
                return
            if self.state == HALF_OPEN:
                self.active_trials = max(0, self.active_trials - 1)
                self.trials_completed += 1
                self.consecutive_successes += 1
                self.consecutive_failures = 0
                self._window.append(True)
                if len(self._window) > self.cfg.error_window_size:
                    self._window.pop(0)
                if self.consecutive_successes >= self.cfg.half_open_success:
                    self._close("trials_succeeded")
                return

            self.consecutive_failures = 0
            self._window.append(True)
            if len(self._window) > self.cfg.error_window_size:
                self._window.pop(0)

    def record_failure(self, reason: str = "failure") -> None:
        with self.lock:
            # OPEN 之后才返回的在途失败直接忽略: 不重复跳闸、不重置冷却
            if self.state == OPEN:
                return
            if self.state == HALF_OPEN:
                self.active_trials = max(0, self.active_trials - 1)
                self.trials_completed += 1
                # 半开期间任何一次异常都立即重新熔断并重置冷却时间
                self._trip_open("trial_failed:" + reason)
                return

            self.consecutive_failures += 1
            self.consecutive_successes = 0
            self._window.append(False)
            if len(self._window) > self.cfg.error_window_size:
                self._window.pop(0)

            if self.consecutive_failures >= self.cfg.failure_threshold:
                self._trip_open("consecutive_failures:" + reason)
                return
            rate = self._error_rate()
            if rate is not None and rate >= self.cfg.error_rate_threshold:
                self._trip_open("error_rate:" + reason, round(rate, 3))

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            self.maybe_half_open()
            rate = self._error_rate()
            remain = max(0.0, self.wake_at - self._clock()) if self.state == OPEN else 0.0
            return {
                "state": self.state,
                "consecutive_failures": self.consecutive_failures,
                "consecutive_successes": self.consecutive_successes,
                "active_trials": self.active_trials,
                "trials_completed": self.trials_completed,
                "open_count": self.open_count,
                "error_rate": round(rate, 3) if rate is not None else None,
                "cooldown_remaining": round(remain, 2),
            }

    def reset(self) -> None:
        with self.lock:
            self.state = CLOSED
            self.consecutive_failures = 0
            self.consecutive_successes = 0
            self.opened_at = 0.0
            self.wake_at = 0.0
            self.active_trials = 0
            self.trials_completed = 0
            self._window.clear()


# ---------------------------------------------------------------------------
# 平滑加权负载均衡器 (nginx smooth weighted round-robin)
# ---------------------------------------------------------------------------


class SmoothWeightedLB:
    """每轮选择 current_weight 最大者, 随后:

        chosen.current -= total_weight
        every.current  += every.effective

    权重差异按比例展开成平滑序列 (如 5:1:1 -> a a b a c a a),
    同一节点不会被连续打爆。
    """

    def __init__(self) -> None:
        self.weights: Dict[str, int] = {}
        self.current: Dict[str, float] = {}

    def update_weights(self, weights: Dict[str, int]) -> None:
        for node_id, weight in weights.items():
            self.weights[node_id] = max(1, int(weight))
            self.current.setdefault(node_id, 0.0)
        for node_id in [n for n in self.current if n not in weights]:
            self.current.pop(node_id, None)
            self.weights.pop(node_id, None)

    def pick(self, eligible: List[str]) -> Optional[str]:
        eligible = [n for n in eligible if n in self.weights]
        if not eligible:
            return None
        total = sum(self.weights[n] for n in eligible)
        for node_id in eligible:
            self.current[node_id] += self.weights[node_id]
        best = max(eligible, key=lambda n: self.current[n])
        self.current[best] -= total
        return best

    def reset(self) -> None:
        self.current = {n: 0.0 for n in self.weights}

# ---------------------------------------------------------------------------
# 节点模型
# ---------------------------------------------------------------------------


@dataclass
class NodeSpec:
    id: str
    name: str
    service: str
    base_latency: float       # 基线往返延迟 (秒)
    static_weight: int       # 初始静态权重
    x: float = 0.0           # 拓扑画布坐标 (0~1)
    y: float = 0.0


class MeshNode:
    """运行期节点: 统计量 + 断路器 + 故障注入开关。"""

    def __init__(self, spec: NodeSpec, cfg: MeshConfig, event_cb) -> None:
        self.spec = spec
        self.cfg = cfg
        self.breaker = CircuitBreaker(spec.id, cfg, on_event=event_cb)

        # 动态指标
        self.latency_ewma = spec.base_latency
        self.health = 1.0
        self.effective_weight = float(spec.static_weight)

        # 窗口统计: [(timestamp, outcome)]
        self.history: List[tuple] = []
        self.total_success = 0
        self.total_timeout = 0
        self.total_error = 0
        self.active_calls = 0

        # 故障注入
        self.inject_delay: float = 0.0   # 叠加延迟 (秒), 0 表示无注入
        self.offline = False
        self.force_error = False

        # 拓扑动效提示 (供前端高亮最近一次命中)
        self.last_hit_at = 0.0

    def record_call(self, outcome: str, latency: float, now: float) -> None:
        self.history.append((now, outcome))
        self.active_calls = max(0, self.active_calls - 1)
        self.last_hit_at = now
        if outcome == SUCCESS:
            self.total_success += 1
            # 延迟 EWMA
            self.latency_ewma = (
                1 - self.cfg.health_alpha
            ) * self.latency_ewma + self.cfg.health_alpha * latency
        elif outcome == TIMEOUT:
            self.total_timeout += 1
            # 超时按超时阈值计入延迟, 拉低健康
            self.latency_ewma = (
                1 - self.cfg.health_alpha
            ) * self.latency_ewma + self.cfg.health_alpha * self.cfg.call_timeout
        else:
            self.total_error += 1


# ---------------------------------------------------------------------------
# 令牌桶
# ---------------------------------------------------------------------------


class TokenBucket:
    """经典令牌桶: 恒定速率补充 + 突发容量。线程安全 (由内核锁保护)。"""

    def __init__(self, rate: float, capacity: int, clock: Callable[[], float] = time.monotonic) -> None:
        self.rate = float(rate)
        self.capacity = float(capacity)
        self._clock = clock
        self.tokens = float(capacity)
        self.updated = clock()

    def configure(self, rate: float, capacity: int) -> None:
        self.rate = float(rate)
        self.capacity = float(capacity)
        self.tokens = min(self.tokens, self.capacity)

    def refill(self) -> None:
        now = self._clock()
        delta = now - self.updated
        if delta > 0:
            self.tokens = min(self.capacity, self.tokens + delta * self.rate)
            self.updated = now

    def try_acquire(self, amount: float = 1.0) -> bool:
        self.refill()
        if self.tokens >= amount:
            self.tokens -= amount
            return True
        return False

# ---------------------------------------------------------------------------
# 流量治理内核
# ---------------------------------------------------------------------------


# 默认拓扑: 网关 -> 两个服务共五个实例
DEFAULT_NODES = [
    NodeSpec("order-1", "订单实例 A", "order-service", 0.035, 10, 0.30, 0.32),
    NodeSpec("order-2", "订单实例 B", "order-service", 0.05, 8, 0.24, 0.62),
    NodeSpec("order-3", "订单实例 C", "order-service", 0.08, 5, 0.36, 0.82),
    NodeSpec("pay-1", "支付实例 A", "payment-service", 0.045, 9, 0.68, 0.34),
    NodeSpec("pay-2", "支付实例 B", "payment-service", 0.07, 6, 0.72, 0.70),
]


class TrafficKernel:
    """异步流量治理仿真内核。

    生命周期: start() 启动后台事件循环并生成常驻协程; stop() 优雅关闭。
    """

    def __init__(
        self,
        config: Optional[MeshConfig] = None,
        node_specs: Optional[List[NodeSpec]] = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.cfg = config or MeshConfig()
        self.clock = clock
        self.lock = threading.RLock()

        self._event_seq = itertools.count(1)
        self._listeners: List[Callable[[Dict[str, Any]], None]] = []
        self._event_ring: List[Dict[str, Any]] = []
        self._RING_MAX = 400

        self.nodes: Dict[str, MeshNode] = {}
        for spec in node_specs or DEFAULT_NODES:
            self.nodes[spec.id] = MeshNode(spec, self.cfg, self._on_breaker_event)

        self.lb = SmoothWeightedLB()
        self.bucket = TokenBucket(self.cfg.rate_limit, self.cfg.burst_capacity, clock)
        self._concurrency_sem: Optional[asyncio.Semaphore] = None

        # 全局计数
        self.global_history: List[tuple] = []
        self.total_sent = 0
        self.total_success = 0
        self.total_timeout = 0
        self.total_error = 0
        self.total_throttled = 0
        self.total_rejected = 0
        self.active_calls = 0

        # 流量控制
        self.target_rps = self.cfg.base_rps
        self._producer_task: Optional[asyncio.Task] = None
        self._weight_task: Optional[asyncio.Task] = None
        self._demo_task: Optional[asyncio.Task] = None
        self.running_demo: Optional[str] = None

        # 事件循环线程
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._ready = threading.Event()
        self._started_at = 0.0

    # == 事件总线 ==========================================================

    def _on_breaker_event(self, kind: str, payload: Dict[str, Any]) -> None:
        # 此回调在事件循环线程内被调用 (也可能来自快照推进, 任意线程)
        self.publish("breaker", payload)

    def publish(self, kind: str, data: Optional[Dict[str, Any]] = None, force: bool = False) -> None:
        with self.lock:
            event = {
                "seq": next(self._event_seq),
                "ts": round(self.clock() - self._started_at, 3) if self._started_at else 0.0,
                "kind": kind,
                "data": data or {},
            }
            self._event_ring.append(event)
            if len(self._event_ring) > self._RING_MAX:
                del self._event_ring[: -self._RING_MAX]
            listeners = list(self._listeners)
        for listener in listeners:
            try:
                listener(event)
            except Exception:
                # 监听器异常绝不能影响内核
                pass

    def add_listener(self, listener: Callable[[Dict[str, Any]], None]) -> Callable[[], None]:
        with self.lock:
            self._listeners.append(listener)

        def _remove() -> None:
            with self.lock:
                if listener in self._listeners:
                    self._listeners.remove(listener)

        return _remove

    def recent_events(self, after_seq: int = 0, limit: int = 300) -> List[Dict[str, Any]]:
        with self.lock:
            events = [e for e in self._event_ring if e["seq"] > after_seq]
        return events[-limit:]

    # == 线程/事件循环桥接 =================================================

    def submit(self, coro: Awaitable) -> Any:
        """把协程线程安全地投递进内核事件循环。"""
        if self._loop is None:
            raise RuntimeError("kernel not started")
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    def start(self) -> None:
        if self._thread is not None:
            return

        def _runner() -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop
            loop.call_soon_threadsafe(self._ready.set)
            loop.run_forever()

        self._thread = threading.Thread(target=_runner, name="mesh-kernel-loop", daemon=True)
        self._thread.start()
        self._ready.wait(timeout=5)
        self._started_at = self.clock()
        self.submit(self._bootstrap()).result(timeout=5)

    def stop(self) -> None:
        if self._loop is None:
            return
        try:
            self.submit(self._shutdown()).result(timeout=5)
        except Exception:
            pass
        try:
            self._loop.call_soon_threadsafe(self._loop.stop)
        except Exception:
            pass
        loop = self._loop
        if self._thread:
            self._thread.join(timeout=3)
        if loop and not loop.is_closed():
            loop.close()
        self._thread = None
        self._loop = None

    async def _bootstrap(self) -> None:
        self._concurrency_sem = asyncio.Semaphore(self.cfg.max_concurrency)
        self._recompute_weights_initial()
        self._producer_task = asyncio.create_task(self._produce_traffic())
        self._weight_task = asyncio.create_task(self._weight_loop())
        self.publish("kernel", {"msg": "mesh kernel started", "nodes": len(self.nodes)})

    async def _shutdown(self) -> None:
        current = asyncio.current_task()
        pending = [
            task for task in asyncio.all_tasks()
            if task is not current and not task.done()
        ]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self.publish("kernel", {"msg": "mesh kernel stopped"})
    # == 动态权重 ==========================================================

    def _recompute_weights_initial(self) -> None:
        with self.lock:
            weights = {nid: node.spec.static_weight for nid, node in self.nodes.items()}
            self.lb.update_weights(weights)

    def _compute_target_weight(self, node: MeshNode) -> int:
        # 延迟因子: 相对同服务基准, 越慢权重越小
        latency_ratio = node.latency_ewma / max(node.spec.base_latency, 1e-3)
        latency_factor = 1.0 / (1.0 + max(0.0, latency_ratio - 1.0) * 2.0)
        # 健康因子: health 0..1
        factor = max(0.08, node.health) * max(0.1, latency_factor)
        target = node.spec.static_weight * factor
        return max(1, int(round(target)))

    async def _weight_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.cfg.weight_interval)
                await self._refresh_weights()
        except asyncio.CancelledError:
            return

    async def _refresh_weights(self) -> None:
        with self.lock:
            new_weights: Dict[str, int] = {}
            alpha = self.cfg.weight_smoothing
            for node_id, node in self.nodes.items():
                target = self._compute_target_weight(node)
                # EWMA 平滑过渡, 避免权重抖动导致流量突跳
                node.effective_weight = (
                    1 - alpha
                ) * node.effective_weight + alpha * target
                new_weights[node_id] = max(1, int(round(node.effective_weight)))
            self.lb.update_weights(new_weights)

    # == 流量生产器 ========================================================

    async def _produce_traffic(self) -> None:
        try:
            tick = 0.05
            while True:
                await asyncio.sleep(tick)
                with self.lock:
                    rps = self.target_rps
                expected = rps * tick
                n = int(expected)
                if random.random() < (expected - n):
                    n += 1
                # 单 tick 上限, 防止异常参数打爆事件循环
                n = min(n, 40)
                for _ in range(n):
                    asyncio.create_task(self._handle_request())
        except asyncio.CancelledError:
            return

    # == 路由 + 调用 =======================================================

    def _eligible_nodes(self) -> List[MeshNode]:
        result = []
        for node in self.nodes.values():
            node.breaker.maybe_half_open()
            if node.offline:
                continue
            if node.breaker.state != OPEN:
                result.append(node)
        return result

    async def _handle_request(self) -> None:
        # 1) 令牌桶限流 (快速失败)
        with self.lock:
            allowed = self.bucket.try_acquire()
        if not allowed:
            with self.lock:
                self.total_throttled += 1
                self.global_history.append((self.clock(), THROTTLED))
                throttled = self.total_throttled
            if throttled % 20 == 1:
                self.publish("throttle", {"reason": "token_bucket_empty", "count": throttled})
            return

        # 2) 全局并发闸门
        assert self._concurrency_sem is not None
        async with self._concurrency_sem:
            # 3) 选择节点: HALF_OPEN 探针优先, 其余平滑加权轮询
            target: Optional[MeshNode] = None
            with self.lock:
                half_open = [
                    n for n in self.nodes.values()
                    if not n.offline and n.breaker.state == HALF_OPEN
                ]
                random.shuffle(half_open)
                for node in half_open:
                    if node.breaker.can_call():
                        target = node
                        break

                if target is None:
                    closed = [
                        n for n in self.nodes.values()
                        if not n.offline and n.breaker.state == CLOSED
                    ]
                    chosen_id = self.lb.pick([n.spec.id for n in closed])
                    if chosen_id:
                        target = self.nodes[chosen_id]

                if target is None:
                    # 所有节点熔断或离线
                    self.total_rejected += 1
                    self.global_history.append((self.clock(), REJECTED))
                    rejected = self.total_rejected
                    self.active_calls += 1
            if target is None:
                if rejected % 20 == 1:
                    self.publish("reject", {"reason": "all_nodes_open", "count": rejected})
                with self.lock:
                    self.active_calls = max(0, self.active_calls - 1)
                return

            is_trial = target.breaker.state == HALF_OPEN
            with self.lock:
                target.active_calls += 1
                self.total_sent += 1
                self.active_calls += 1

            outcome, latency = await self._call_node(target)

            with self.lock:
                self.active_calls = max(0, self.active_calls - 1)
                target.record_call(outcome, latency, self.clock())
                self.global_history.append((self.clock(), outcome))
                if outcome == SUCCESS:
                    self.total_success += 1
                    target.breaker.record_success()
                    self._update_health_on_success(target)
                elif outcome == TIMEOUT:
                    self.total_timeout += 1
                    self._update_health_on_failure(target)
                    target.breaker.record_failure(TIMEOUT)
                else:
                    self.total_error += 1
                    self._update_health_on_failure(target)
                    target.breaker.record_failure(ERROR)

            self.publish(
                "route",
                {
                    "node": target.spec.id,
                    "service": target.spec.service,
                    "outcome": outcome,
                    "latency_ms": round(latency * 1000, 1),
                    "trial": is_trial,
                    "state": target.breaker.state,
                },
            )

    async def _call_node(self, node: MeshNode) -> tuple:
        """模拟一次下游调用 (延迟/离线/异常/超时), 不持锁。"""
        delay_base = node.spec.base_latency + node.inject_delay
        jitter = random.uniform(0.7, 1.3)
        latency = delay_base * jitter

        if node.offline:
            return ERROR, self.cfg.call_timeout
        if node.force_error:
            return ERROR, latency

        sleep_for = latency
        timed_out = False
        try:
            await asyncio.wait_for(asyncio.sleep(sleep_for), timeout=self.cfg.call_timeout)
        except asyncio.TimeoutError:
            timed_out = True
            latency = self.cfg.call_timeout

        # 高负载注入延迟期间叠加少量随机错误 (使错误率通道也可触发)
        if not timed_out and node.inject_delay > 0:
            if random.random() < 0.15:
                return ERROR, latency
        if timed_out:
            return TIMEOUT, latency
        return SUCCESS, latency

    # == 健康度 ============================================================

    def _update_health_on_success(self, node: MeshNode) -> None:
        # 延迟越接近基线越健康
        ratio = min(3.0, node.latency_ewma / max(node.spec.base_latency, 1e-3))
        latency_health = max(0.0, 1.0 - (ratio - 1.0) * 0.8)
        target = 0.25 + 0.75 * latency_health
        node.health = (1 - self.cfg.health_alpha) * node.health + self.cfg.health_alpha * target
        node.health = min(1.0, node.health)

    def _update_health_on_failure(self, node: MeshNode) -> None:
        node.health = (1 - self.cfg.health_alpha) * node.health + self.cfg.health_alpha * 0.1
        node.health = max(0.0, node.health)
    # == 公共控制 API (线程安全) ===========================================

    def set_traffic(self, rps: int) -> None:
        with self.lock:
            self.target_rps = max(0, int(rps))
        self.publish("control", {"action": "set_traffic", "rps": self.target_rps}, force=True)

    def burst(self, duration: float = 2.5, rps: int = 320) -> None:
        self.publish("control", {"action": "burst_start", "rps": rps, "duration": duration}, force=True)
        self.submit(self._burst_coro(duration, rps))

    async def _burst_coro(self, duration: float, rps: int) -> None:
        with self.lock:
            old = self.target_rps
            self.target_rps = rps
        await asyncio.sleep(duration)
        with self.lock:
            # 若期间没有其它演示改流量, 恢复常态
            if self.target_rps == rps:
                self.target_rps = old
        self.publish("control", {"action": "burst_end", "rps": old}, force=True)

    def inject_fault(self, node_id: str, delay: float = 0.3, force_error: bool = False, offline: bool = False) -> None:
        self.submit(self._inject_fault_coro(node_id, delay, force_error, offline))

    async def _inject_fault_coro(self, node_id, delay, force_error, offline) -> None:
        with self.lock:
            node = self.nodes.get(node_id)
            if node is None:
                return
            node.inject_delay = delay
            node.force_error = force_error
            node.offline = offline
        self.publish(
            "fault",
            {"node": node_id, "inject_delay": delay, "force_error": force_error, "offline": offline},
            force=True,
        )

    def recover_node(self, node_id: str) -> None:
        self.inject_fault(node_id, delay=0.0, force_error=False, offline=False)

    def reset_breakers(self) -> None:
        with self.lock:
            for node in self.nodes.values():
                node.breaker.reset()
                node.health = 1.0
                node.latency_ewma = node.spec.base_latency
                node.effective_weight = float(node.spec.static_weight)
            self.lb.reset()
        self.publish("control", {"action": "reset_breakers"}, force=True)

    # == 一键演示编排 ======================================================

    def start_demo(self, name: str) -> bool:
        name = name if name in ("normal", "circuit", "full") else "full"
        with self.lock:
            if self._demo_task and not self._demo_task.done():
                return False
            self.running_demo = name
        # 可能从 HTTP/测试线程调用, 显式投递到内核事件循环
        future = self.submit(self._spawn_demo(name))
        try:
            future.result(timeout=2)
        except Exception:
            with self.lock:
                self.running_demo = None
            return False
        return True

    async def _spawn_demo(self, name: str) -> None:
        self._demo_task = asyncio.create_task(self._demo_runner(name))

        def _done(_task):
            with self.lock:
                self.running_demo = None

        self._demo_task.add_done_callback(_done)

    async def _demo_step(self, phase: str, delay: float, action: Optional[Callable] = None) -> None:
        self.publish("demo", {"phase": phase}, force=True)
        if action:
            action()
        await asyncio.sleep(delay)

    async def _demo_runner(self, name: str) -> None:
        try:
            if name == "normal":
                await self._demo_normal()
            elif name == "circuit":
                await self._demo_circuit()
            else:
                await self._demo_full()
        except asyncio.CancelledError:
            self.publish("demo", {"phase": "cancelled"}, force=True)
            raise
        finally:
            self.running_demo = None

    async def _demo_normal(self) -> None:
        with self.lock:
            base = self.cfg.base_rps
        await self._demo_step("常态流量: 按动态权重平滑分发", 1.5,
                              lambda: self.set_traffic(base))
        await self._demo_step("突发洪峰: 令牌桶削峰, 超额请求快速失败", 3.2,
                              lambda: self.burst(3.0, 320))
        await self._demo_step("洪峰退去: 吞吐恢复常态", 2.0,
                              lambda: self.set_traffic(base))
        await self._demo_step("normal-demo-done", 0.1)

    async def _demo_circuit(self) -> None:
        target = "order-3"
        with self.lock:
            base = self.cfg.base_rps
        await self._demo_step("准备: 复位断路器, 五节点均衡承载", 1.2,
                              lambda: (self.reset_breakers(), self.set_traffic(base)))
        await self._demo_step("对 order-3 注入 350ms 延迟 (超过 220ms 超时阈值)", 1.0,
                              lambda: (self.recover_node(target), self.inject_fault(target, delay=0.35)))
        await self._demo_step("连续超时累积, order-3 触发熔断 -> OPEN, 流量立即切除", 3.2)
        await self._demo_step("OPEN 静默窗口内请求快速失败并重定向到健康节点", 5.0)
        await self._demo_step("冷却结束 -> HALF_OPEN, 放行单飞试探流量", 2.5)
        await self._demo_step("解除故障: 探针连续成功, 断路器自愈回 CLOSED", 1.0,
                              lambda: self.recover_node(target))
        await self._demo_step("order-3 恢复接流, 权重随健康度平滑回升", 4.0)
        await self._demo_step("circuit-demo-done", 0.1)

    async def _demo_full(self) -> None:
        target = "order-3"
        with self.lock:
            base = self.cfg.base_rps
        await self._demo_step("阶段 1/5: 集群常态运行, 动态权重平滑分流", 2.0,
                              lambda: (self.reset_breakers(), self.set_traffic(base)))
        await self._demo_step("阶段 2/5: 突发洪峰到来, 令牌桶削峰限流", 3.0,
                              lambda: self.burst(2.6, 320))
        await self._demo_step("阶段 3/5: order-3 网络劣化, 注入 350ms 延迟", 1.0,
                              lambda: (self.set_traffic(base), self.inject_fault(target, delay=0.35)))
        await self._demo_step("阶段 4/5: 超时/错误越限 -> OPEN, 流量切除并重定向", 4.5)
        await self._demo_step("阶段 5a: 静默冷却中, 该节点请求被快速失败拦截", 4.0)
        await self._demo_step("阶段 5b: 冷却结束 -> HALF_OPEN, 试探流量单飞放行", 2.5)
        await self._demo_step("阶段 5c: 故障未恢复前探针失败 -> 重新 OPEN 并重置冷却", 4.5)
        await self._demo_step("阶段 5d: 网络恢复, 再次半开探针连续成功 -> CLOSED 自愈", 1.0,
                              lambda: self.recover_node(target))
        await self._demo_step("自愈完成: order-3 重新接流, 回归全量均衡", 4.0)
        await self._demo_step("full-demo-done", 0.1)
    # == 状态快照 (线程安全, 供 Web/API 读取) ==============================

    @staticmethod
    def _window_counters(history: List[tuple], now: float, window: float = 1.0) -> Dict[str, int]:
        cutoff = now - window
        counters = {"success": 0, "timeout": 0, "error": 0,
                    "throttled": 0, "rejected": 0}
        # history 近似有序, 从尾部扫描
        for ts, outcome in reversed(history):
            if ts < cutoff:
                break
            if outcome in counters:
                counters[outcome] += 1
        return counters

    def snapshot(self) -> Dict[str, Any]:
        now = self.clock()
        with self.lock:
            self.bucket.refill()
            uptime = round(now - self._started_at, 2) if self._started_at else 0.0
            counters = self._window_counters(self.global_history, now)
            rps = sum(counters.values())
            nodes_out = []
            for node in self.nodes.values():
                c = self._window_counters(node.history, now)
                br = node.breaker.snapshot()
                nodes_out.append({
                    "id": node.spec.id,
                    "name": node.spec.name,
                    "service": node.spec.service,
                    "x": node.spec.x,
                    "y": node.spec.y,
                    "health": round(node.health, 3),
                    "latency_ms": round(node.latency_ewma * 1000, 1),
                    "base_latency_ms": round(node.spec.base_latency * 1000, 1),
                    "weight": int(round(node.effective_weight)),
                    "static_weight": node.spec.static_weight,
                    "active_calls": node.active_calls,
                    "rps": c["success"] + c["timeout"] + c["error"],
                    "ok_rps": c["success"],
                    "success": node.total_success,
                    "timeout": node.total_timeout,
                    "error": node.total_error,
                    "inject_delay_ms": round(node.inject_delay * 1000, 1),
                    "offline": node.offline,
                    "force_error": node.force_error,
                    "breaker": br,
                    "last_hit_age": round(now - node.last_hit_at, 2) if node.last_hit_at else 999,
                })
            return {
                "uptime": uptime,
                "target_rps": self.target_rps,
                "measured_rps": rps,
                "ok_rps": counters["success"],
                "timeout_rps": counters["timeout"],
                "error_rps": counters["error"],
                "throttle_rps": counters["throttled"],
                "reject_rps": counters["rejected"],
                "tokens": round(self.bucket.tokens, 1),
                "rate_limit": self.cfg.rate_limit,
                "burst_capacity": self.cfg.burst_capacity,
                "active_calls": self.active_calls,
                "max_concurrency": self.cfg.max_concurrency,
                "totals": {
                    "sent": self.total_sent,
                    "success": self.total_success,
                    "timeout": self.total_timeout,
                    "error": self.total_error,
                    "throttled": self.total_throttled,
                    "rejected": self.total_rejected,
                },
                "running_demo": self.running_demo,
                "call_timeout_ms": round(self.cfg.call_timeout * 1000),
                "open_cooldown": self.cfg.open_cooldown,
                "nodes": nodes_out,
            }


# ---------------------------------------------------------------------------
# python mesh_kernel.py 直接运行时的最小冒烟自测
# ---------------------------------------------------------------------------

def _smoke() -> None:
    cfg = MeshConfig(rate_limit=200, open_cooldown=2.0)
    kernel = TrafficKernel(cfg)
    kernel.start()
    try:
        time.sleep(1.0)
        kernel.burst(1.0, 500)
        time.sleep(2.5)
        snap = kernel.snapshot()
        print("rps=%s sent=%s throttled=%s nodes=%s" % (
            snap["measured_rps"], snap["totals"]["sent"],
            snap["totals"]["throttled"], len(snap["nodes"])))
    finally:
        kernel.stop()


if __name__ == "__main__":
    _smoke()