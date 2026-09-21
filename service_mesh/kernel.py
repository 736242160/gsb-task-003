"""Asynchronous, thread-safe service mesh simulation kernel.

The kernel models a gateway fronting two services ("order" -> "pay"),
each backed by several instance nodes.  It implements:

* smooth weighted round-robin load balancing (Nginx-style SWRR) with
  dynamic weights driven by EWMA latency and a node health score;
* a token-bucket rate limiter at the gateway (burst shaping) plus a
  per-node bulkhead (max in-flight) to contain overload;
* three-state circuit breakers (see :mod:`service_mesh.breaker`) with
  fast-fail rejection and half-open probe recovery;
* one failover retry on an isolated node failure;
* an optional low-level background traffic generator;
* JSON snapshots and a bounded event log consumed by the web console.

All shared state is guarded by a single re-entrant lock; every blocking
point is ``await``-based asyncio, so the kernel is safe to drive from
multiple threads (the web server invokes it via ``run_coroutine_threadsafe``).
"""

import asyncio
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from .breaker import CircuitBreaker, CLOSED, OPEN, HALF_OPEN
from .events import EventLog

REQUEST_OK = "OK"
REQUEST_TIMEOUT = "TIMEOUT"
REQUEST_ERROR = "ERROR"
REQUEST_REJECTED = "REJECTED"
REQUEST_FALLBACK = "FALLBACK"


class Node:
    """A simulated service instance (host:port)."""

    def __init__(self, node_id, service, host, base_latency, weight=10):
        self.id = node_id
        self.service = service
        self.host = host
        self.base_latency = float(base_latency)
        self.base_weight = float(weight)

        # dynamic runtime state
        self.health = 100.0               # 0..100 health score
        self.ewma_latency = float(base_latency)
        self.inflight = 0
        self.injected_delay = 0.0         # fault injection
        self.fail_injection = 0.0         # forced failure probability
        self.offline = False              # administrative kill / hard down

        # cumulative counters
        self.total_requests = 0
        self.success = 0
        self.failures = 0
        self.timeouts = 0
        self.rejected = 0                 # bulkhead rejections
        self.breaker_rejected = 0         # fast-fail / probe rejections

        # per-second throughput buckets: deque[(timestamp, ok, fail, rejected)]
        from collections import deque

        self._rps = deque()

        # smooth weighted round-robin scratch
        self.current_weight = 0.0
        self.breaker = None


class Service:
    """A named service tier with its instance pool."""

    def __init__(self, name, timeout=0.5, max_inflight=40):
        self.name = name
        self.nodes = []
        self.timeout = timeout
        self.max_inflight = max_inflight
        self.total = 0
        self.ok = 0
        self.failed = 0
        self.fallback = 0


class TokenBucket:
    """Async token bucket (burst-capable rate limiter), coroutine-local."""

    def __init__(self, rate, burst):
        self.rate = float(rate)
        self.burst = float(burst)
        self.tokens = float(burst)
        self.updated = time.monotonic()

    def _refill(self, now):
        elapsed = now - self.updated
        if elapsed > 0:
            self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
            self.updated = now

    async def acquire(self, max_wait=0.25, sleeper=None):
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max_wait
        while True:
            now = time.monotonic()
            self._refill(now)
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return True
            needed = (1.0 - self.tokens) / self.rate
            if loop.time() + needed > deadline:
                # drop the partial accrual benefit: immediate rejection
                return False
            sleep_for = min(needed, max(0.0, deadline - loop.time()))
            if sleeper is not None:
                await sleeper(sleep_for)
            else:
                await asyncio.sleep(max(0.001, sleep_for))


class MeshKernel:
    """The simulation kernel. One instance backs the whole console."""

    def __init__(self, clock=None, config=None, loop=None):
        config = config or DEFAULT_CONFIG
        self._cfg = config
        self._lock = threading.RLock()
        self._clock = clock
        self._rng = random.Random(0xC0FFEE)
        self.events = EventLog(clock=clock)

        self.services = {}
        self.nodes = {}
        self._build_topology(config)

        # gateway rate limiter
        rb = config.get("rate_limit", {})
        self.limiter = TokenBucket(
            rate=rb.get("rate", 60.0), burst=rb.get("burst", 30.0)
        )
        self.rate_max_wait = rb.get("max_wait", 0.15)
        self.gateway_rps_target = 0.0

        # gateway counters
        self.gateway_total = 0
        self.gateway_ok = 0
        self.gateway_failed = 0
        self.gateway_rejected = 0
        self.gateway_fallback = 0
        self.gateway_inflight = 0
        from collections import deque

        self._gateway_rps = deque()
        self._rps_lock = threading.RLock()

        # breaker rejection log throttle (per node)
        self._last_breaker_log = {}

        # asyncio plumbing
        self.loop = loop
        self._executor = ThreadPoolExecutor(max_workers=16, thread_name_prefix="mesh")
        self._bg_tasks = set()
        self._stop_event = None
        self._traffic_task = None
        self._reporter_task = None
        self.traffic_rate = 0.0
        self.started_at = None

    # ------------------------------------------------------------------
    def _build_topology(self, config):
        cb_cfg = config.get("breaker", {})
        for svc_cfg in config["services"]:
            svc = Service(
                svc_cfg["name"],
                timeout=svc_cfg.get("timeout", 0.5),
                max_inflight=svc_cfg.get("max_inflight", 40),
            )
            for n in svc_cfg["nodes"]:
                node = Node(
                    n["id"], svc.name, n["host"],
                    base_latency=n.get("latency", 0.05),
                    weight=n.get("weight", 10),
                )
                node.breaker = CircuitBreaker(
                    n["id"], clock=self._clock,
                    error_rate_threshold=cb_cfg.get("error_rate_threshold", 0.5),
                    min_samples=cb_cfg.get("min_samples", 4),
                    consecutive_failures=cb_cfg.get("consecutive_failures", 3),
                    cooldown=cb_cfg.get("cooldown", 6.0),
                    half_open_probes=cb_cfg.get("half_open_probes", 2),
                    error_window=cb_cfg.get("error_window", 10.0),
                )
                svc.nodes.append(node)
                self.nodes[node.id] = node
            self.services[svc.name] = svc

    def now(self):
        if self._clock is not None:
            return float(self._clock())
        return time.monotonic()

    # ------------------------------------------------------------------
    # thread-safe coroutine entry point (called from http server threads)
    def submit(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def call(self, method_name, *args, **kwargs):
        """Invoke an async kernel method from another thread synchronously."""
        future = self.submit(getattr(self, method_name)(*args, **kwargs))
        return future.result()

    # ------------------------------------------------------------------
    # dynamic weights & smooth weighted round robin
    # ------------------------------------------------------------------
    def effective_weight(self, node):
        latency = max(node.ewma_latency, 1e-3)
        latency_ratio = node.base_latency / latency
        latency_factor = min(2.0, max(0.2, latency_ratio))
        weight = node.base_weight * latency_factor * (node.health / 100.0)
        return max(0.3, weight)

    def route(self, service_name):
        """Select a node for *service_name*.

        Returns ``(node, reason)``.  ``reason`` is None on success or one of
        ``"breaker-open"`` / ``"bulkhead-full"`` / ``"no-nodes"``.
        SWRR current weights are mutated while the service lock is held.
        """
        svc = self.services[service_name]
        with self._lock:
            candidates = [
                n for n in svc.nodes
                if not n.offline
            ]
            if not candidates:
                return None, "no-nodes"

            total = 0.0
            best = None
            probed = []
            for node in candidates:
                allowed, transition = node.breaker.allow_request()
                if transition is not None:
                    self._on_breaker_transition(node, transition)
                if not allowed:
                    now0 = self.now()
                    node.breaker_rejected += 1
                    self._record_rps(node, now0, 0, 0, 1)
                    self._log_breaker_rejection(node)
                    continue
                if node.inflight >= svc.max_inflight:
                    continue
                if node.breaker.state == HALF_OPEN:
                    probed.append(node)
                weight = self.effective_weight(node)
                node.current_weight += weight
                total += weight
                if best is None or node.current_weight > best.current_weight:
                    best = node

            if best is None:
                # distinguish the dominant cause for the event log
                any_probe_slot = any(
                    n.breaker.state == HALF_OPEN or n.breaker.state == CLOSED
                    for n in candidates
                )
                reason = "bulkhead-full" if any_probe_slot else "breaker-open"
                for candidate in candidates:
                    if candidate.breaker.state == HALF_OPEN:
                        candidate.breaker.release_probe()
                return None, reason

            for node in probed:
                if node is not best:
                    node.breaker.release_probe()
            best.current_weight -= total
            return best, None

    # ------------------------------------------------------------------
    # circuit breaker bookkeeping
    # ------------------------------------------------------------------
    def _on_breaker_transition(self, node, transition):
        kind, detail = transition
        if kind == "CLOSED->OPEN":
            self.events.emit(
                "ERROR", "breaker",
                "node %s TRIPPED OPEN (%s) - fast-fail active" % (node.id, detail),
                node=node.id, service=node.service,
            )
        elif kind == "OPEN->HALF_OPEN":
            self.events.emit(
                "WARN", "breaker",
                "node %s cooldown elapsed -> HALF-OPEN, probe admitted" % node.id,
                node=node.id, service=node.service,
            )
        elif kind == "HALF_OPEN->CLOSED":
            self.events.emit(
                "OK", "breaker",
                "node %s probes healthy -> CLOSED, full traffic restored" % node.id,
                node=node.id, service=node.service,
            )

    def _note_breaker_open(self, node, detail):
        # record_failure / record_success return transition tuples
        if detail:
            self._on_breaker_transition(node, detail)

    def _log_breaker_rejection(self, node):
        # throttle identical fast-fail spam: at most one log per 0.8s/node
        now = self.now()
        last = self._last_breaker_log.get(node.id, 0.0)
        if now - last >= 0.8:
            self._last_breaker_log[node.id] = now
            snap = node.breaker.snapshot()
            remain = snap["cooldown_remaining"]
            tail = ""
            if remain is not None:
                tail = ", retry in %.1fs" % remain
            self.events.emit(
                "REJECT", "breaker",
                "node %s %s -> request fast-failed%s" % (node.id, snap["state"], tail),
                node=node.id, service=node.service,
            )

    # ------------------------------------------------------------------
    # simulated network call to one node
    # ------------------------------------------------------------------
    async def _call_node(self, node, timeout, sleeper=None):
        sleep = sleeper or asyncio.sleep
        with self._lock:
            node.inflight += 1
            node.total_requests += 1
            injected = node.injected_delay
            fail_chance = node.fail_injection
            base = node.base_latency
        try:
            forced = self._rng.random() < fail_chance
            # forced errors surface quickly; injected delay stretches latency
            jitter = self._rng.uniform(-1.0, 1.0) * max(0.002, 0.2 * base)
            latency = 0.005 if forced else max(0.001, base + injected + jitter)
            await sleep(latency)
            now = self.now()

            with self._lock:
                if forced:
                    node.failures += 1
                    self._record_rps(node, now, ok=0, fail=1, rejected=0)
                    return REQUEST_ERROR, latency, "injected error"
                if latency > timeout:
                    node.timeouts += 1
                    node.failures += 1
                    self._record_rps(node, now, ok=0, fail=1, rejected=0)
                    return REQUEST_TIMEOUT, latency, "latency %.0fms > timeout" % (latency * 1000)
                node.success += 1
                # EWMA latency update (alpha 0.25)
                node.ewma_latency = 0.75 * node.ewma_latency + 0.25 * latency
                # health recovers on success; severe latency slows recovery
                recovery = 2.0 if latency <= node.base_latency * 1.5 else 0.3
                node.health = min(100.0, node.health + recovery)
                self._record_rps(node, now, ok=1, fail=0, rejected=0)
                return REQUEST_OK, latency, None
        finally:
            with self._lock:
                node.inflight = max(0, node.inflight - 1)

    # ------------------------------------------------------------------
    # one load-balanced hop with breaker integration and one failover retry
    # ------------------------------------------------------------------
    async def _do_hop(self, service_name, request_id, allow_fallback=True, sleeper=None):
        svc = self.services[service_name]
        attempts = 0
        last_outcome = None
        with self._lock:
            svc.total += 1

        while attempts < 2:
            attempts += 1
            node, reason = self.route(service_name)
            if node is None:
                with self._lock:
                    svc.failed += 1
                    if reason == "no-nodes":
                        svc.fallback += 1
                if reason == "no-nodes" and allow_fallback:
                    return REQUEST_FALLBACK, None, "no healthy nodes -> degraded response"
                return REQUEST_REJECTED, None, "circuit open / bulkhead full at %s" % service_name

            outcome, latency, detail = await self._call_node(node, svc.timeout, sleeper)
            now = self.now()
            if outcome == REQUEST_OK:
                transition = node.breaker.record_success()
                if transition:
                    self._on_breaker_transition(node, transition)
                with self._lock:
                    svc.ok += 1
                return REQUEST_OK, node, latency

            # failure path: feed the breaker
            trip_reason = node.breaker.record_failure(
                reason=("timeout" if outcome == REQUEST_TIMEOUT else "error")
            )
            if trip_reason:
                self._note_breaker_open(node, trip_reason)
            with self._lock:
                node.health = max(0.0, node.health - (18.0 if outcome == REQUEST_TIMEOUT else 12.0))
            last_outcome = (outcome, node, latency, detail)
            self.events.emit(
                "WARN" if attempts == 1 else "ERROR",
                "call",
                "req %s %s on %s failed (%s)%s"
                % (request_id, service_name, node.id, detail or outcome,
                   " -> failover retry" if attempts == 1 else ""),
                node=node.id, service=service_name,
            )
            if attempts == 1:
                continue
            break

        with self._lock:
            svc.failed += 1
        if allow_fallback:
            with self._lock:
                svc.fallback += 1
            return REQUEST_FALLBACK, last_outcome[1], "%s exhausted retries -> fallback" % service_name
        return last_outcome[0], last_outcome[1], last_outcome[2]

    # ------------------------------------------------------------------
    # throughput bookkeeping (1s sliding buckets)
    # ------------------------------------------------------------------
    def _record_rps(self, node, now, ok, fail, rejected):
        bucket = node._rps
        bucket.append((now, ok, fail, rejected))
        cutoff = now - 10.0
        while bucket and bucket[0][0] < cutoff:
            bucket.popleft()

    def _record_gateway_rps(self, now, ok, fail, rejected):
        bucket = self._gateway_rps
        bucket.append((now, ok, fail, rejected))
        cutoff = now - 10.0
        while bucket and bucket[0][0] < cutoff:
            bucket.popleft()

    @staticmethod
    def _rps_of(bucket, now, window=1.0):
        cutoff = now - window
        ok = fail = rej = 0
        for ts, o, f, r in bucket:
            if ts >= cutoff:
                ok += o
                fail += f
                rej += r
        return {"ok": ok, "failed": fail, "rejected": rej, "total": ok + fail + rej}

    # ------------------------------------------------------------------
    # full gateway request pipeline
    # ------------------------------------------------------------------
    async def handle_request(self, request_id=None, sleeper=None, trace=False):
        if request_id is None:
            request_id = "%04x" % (self._rng.randint(0, 0xFFFF))
        with self._lock:
            self.gateway_total += 1
            self.gateway_inflight += 1

        acquired = await self.limiter.acquire(max_wait=self.rate_max_wait, sleeper=sleeper)
        if not acquired:
            with self._lock:
                self.gateway_rejected += 1
                self.gateway_inflight -= 1
                self._record_gateway_rps(self.now(), 0, 0, 1)
            self.events.emit(
                "REJECT", "ratelimit",
                "req %s rejected at gateway token bucket (burst shaping)" % request_id,
            )
            return {"id": request_id, "result": REQUEST_REJECTED, "hops": []}

        hops = []
        partial = False
        try:
            outcome, node, latency = await self._do_hop("order", request_id, True, sleeper)
            hops.append({"service": "order", "result": outcome,
                         "node": node.id if node else None, "latency": latency})
            if outcome == REQUEST_OK:
                outcome2, node2, latency2 = await self._do_hop("pay", request_id, True, sleeper)
                hops.append({"service": "pay", "result": outcome2,
                             "node": node2.id if node2 else None, "latency": latency2})
                final = outcome2
            else:
                final = outcome
                partial = True

            with self._lock:
                now = self.now()
                if final == REQUEST_OK:
                    self.gateway_ok += 1
                    self._record_gateway_rps(now, 1, 0, 0)
                elif final == REQUEST_FALLBACK:
                    self.gateway_fallback += 1
                    self._record_gateway_rps(now, 0, 1, 0)
                else:
                    self.gateway_failed += 1
                    self._record_gateway_rps(now, 0, 1, 0)
            if trace:
                self.events.emit(
                    "DEBUG", "request",
                    "req %s -> %s %s" % (request_id, final,
                                         " (partial degrade)" if partial else ""),
                )
            return {"id": request_id, "result": final, "hops": hops}
        finally:
            with self._lock:
                self.gateway_inflight -= 1

    # ------------------------------------------------------------------
    # background traffic generator
    # ------------------------------------------------------------------
    async def _traffic_loop(self):
        task = asyncio.current_task()
        while not getattr(task, "_cancel", False):
            rate = self.traffic_rate
            if rate <= 0:
                await asyncio.sleep(0.1)
                continue
            # burst behavior: occasional spikes up to 3x the base rate
            spike = 1.0
            if self._rng.random() < 0.04:
                spike = self._rng.uniform(2.0, 3.2)
            batch = max(1, int(round(rate * spike / 10.0)))
            for _ in range(batch):
                if getattr(task, "_cancel", False):
                    break
                asyncio.ensure_future(self.handle_request())
            await asyncio.sleep(0.1)

    def set_traffic(self, rate):
        with self._lock:
            self.traffic_rate = float(rate)
        if rate > 0:
            self.events.emit("INFO", "traffic", "background traffic set to %.0f rps" % rate)
        else:
            self.events.emit("INFO", "traffic", "background traffic stopped")

    async def _burst(self, count):
        self.events.emit(
            "WARN", "traffic",
            "BURST: firing %d concurrent requests at gateway" % count,
        )
        results = await asyncio.gather(
            *[self.handle_request(request_id="b%d" % i) for i in range(count)]
        )
        summary = {}
        for r in results:
            summary[r["result"]] = summary.get(r["result"], 0) + 1
        self.events.emit(
            "INFO", "traffic",
            "burst settled: " + ", ".join("%s=%d" % kv for kv in sorted(summary.items())),
        )
        return summary

    def burst(self, count=120):
        return self.submit(self._burst(count)).result()

    # ------------------------------------------------------------------
    # periodic one-line throughput reporter
    # ------------------------------------------------------------------
    async def _reporter_loop(self, task=None):
        task = task or asyncio.current_task()
        last_rl = 0
        while not getattr(task, "_cancel", False):
            await asyncio.sleep(1.0)
            with self._lock:
                now = self.now()
                gw = self._rps_of(self._gateway_rps, now)
                parts = []
                for name in sorted(self.services):
                    svc = self.services[name]
                    total = sum(
                        self._rps_of(n._rps, now)["total"] for n in svc.nodes
                    )
                    open_nodes = [n.id for n in svc.nodes if n.breaker.state == OPEN]
                    parts.append("%s:%drps%s" % (
                        name, total,
                        (" [OPEN: " + ",".join(open_nodes) + "]") if open_nodes else "",
                    ))
                rl_rejected = self.gateway_rejected - last_rl
                last_rl = self.gateway_rejected
            if gw["total"] or rl_rejected:
                self.events.emit(
                    "DEBUG", "stats",
                    "gateway %d ok/%d fail/%d shed | %s"
                    % (gw["ok"], gw["failed"], rl_rejected, " | ".join(parts)),
                )

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    def start(self):
        if self.loop is not None:
            return
        self.loop = asyncio.new_event_loop()
        thread = threading.Thread(target=self._run_loop, name="mesh-asyncio", daemon=True)
        self._loop_thread = thread
        thread.start()
        self.started_at = time.time()
        return self.submit(self._start_tasks()).result(timeout=5)

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    async def _start_tasks(self):
        self._stop_event = asyncio.Event()
        reporter = self.loop.create_task(self._reporter_loop())
        traffic = self.loop.create_task(self._traffic_loop())
        self._reporter_task = reporter
        self._traffic_task = traffic
        self._bg_tasks.update([reporter, traffic])

    async def _shutdown(self):
        for task in list(self._bg_tasks):
            task._cancel = True
            task.cancel()
        for task in list(self._bg_tasks):
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._bg_tasks.clear()

    def stop(self):
        if self.loop is None:
            return
        loop = self.loop
        try:
            self.submit(self._shutdown()).result(timeout=5)
        except Exception:
            pass
        loop.call_soon_threadsafe(loop.stop)
        self._executor.shutdown(wait=False)
        thread = getattr(self, "_loop_thread", None)
        if thread is not None:
            thread.join(timeout=2)
        loop.close()
        self.loop = None

    # ------------------------------------------------------------------
    # fault injection & reset
    # ------------------------------------------------------------------
    def inject_latency(self, node_id, delay):
        node = self.nodes[node_id]
        with self._lock:
            node.injected_delay = float(delay)
            if delay > 0:
                self.events.emit(
                    "WARN", "fault",
                    "injecting +%.0fms latency into %s" % (delay * 1000, node_id),
                    node=node_id, service=node.service,
                )
            else:
                self.events.emit(
                    "OK", "fault",
                    "latency injection cleared on %s" % node_id,
                    node=node_id, service=node.service,
                )

    def inject_fault(self, node_id, probability):
        node = self.nodes[node_id]
        with self._lock:
            node.fail_injection = float(probability)
            if probability > 0:
                self.events.emit(
                    "ERROR", "fault",
                    "injecting %.0f%% forced errors into %s" % (probability * 100, node_id),
                    node=node_id, service=node.service,
                )

    def set_offline(self, node_id, offline):
        node = self.nodes[node_id]
        with self._lock:
            node.offline = bool(offline)
            self.events.emit(
                "WARN" if offline else "OK", "fault",
                "node %s %s" % (node_id, "taken OFFLINE" if offline else "back ONLINE"),
                node=node_id, service=node.service,
            )

    def reset(self):
        with self._lock:
            for node in self.nodes.values():
                node.health = 100.0
                node.ewma_latency = node.base_latency
                node.injected_delay = 0.0
                node.fail_injection = 0.0
                node.offline = False
                node.inflight = 0
                node.total_requests = 0
                node.success = 0
                node.failures = 0
                node.timeouts = 0
                node.rejected = 0
                node.breaker_rejected = 0
                node.current_weight = 0.0
                node._rps.clear()
                node.breaker.reset()
            for svc in self.services.values():
                svc.total = 0
                svc.ok = 0
                svc.failed = 0
                svc.fallback = 0
            self.gateway_total = 0
            self.gateway_ok = 0
            self.gateway_failed = 0
            self.gateway_rejected = 0
            self.gateway_fallback = 0
            self.gateway_inflight = 0
            self.traffic_rate = 0.0
            self._gateway_rps.clear()
            self._last_breaker_log.clear()
            self.limiter.tokens = self.limiter.burst
        self.events.clear()
        self.events.emit("OK", "system", "kernel reset: all breakers CLOSED, counters zeroed")

    # ------------------------------------------------------------------
    # JSON snapshot for the web console
    # ------------------------------------------------------------------
    def snapshot(self):
        now = self.now()
        with self._lock:
            nodes_out = []
            for node in self.nodes.values():
                rps1 = self._rps_of(node._rps, now, 1.0)
                rps5 = self._rps_of(node._rps, now, 5.0)
                snap = node.breaker.snapshot()
                nodes_out.append({
                    "id": node.id,
                    "service": node.service,
                    "host": node.host,
                    "state": snap["state"],
                    "health": round(node.health, 1),
                    "inflight": node.inflight,
                    "base_latency": round(node.base_latency * 1000),
                    "ewma_latency": round(node.ewma_latency * 1000),
                    "injected_delay": round(node.injected_delay * 1000),
                    "weight": round(self.effective_weight(node), 2),
                    "base_weight": node.base_weight,
                    "offline": node.offline,
                    "rps": rps1["total"],
                    "rps5": rps5["total"],
                    "failed_rps": rps1["failed"],
                    "rejected_rps": rps1["rejected"],
                    "total_requests": node.total_requests,
                    "success": node.success,
                    "failures": node.failures,
                    "timeouts": node.timeouts,
                    "breaker_rejected": node.breaker_rejected,
                    "failure_rate": snap["failure_rate"],
                    "cooldown_remaining": snap["cooldown_remaining"],
                })
            services_out = []
            for name in sorted(self.services):
                svc = self.services[name]
                total_rps = sum(
                    self._rps_of(n._rps, now)["total"] for n in svc.nodes
                )
                services_out.append({
                    "name": name,
                    "nodes": [n.id for n in svc.nodes],
                    "timeout": svc.timeout,
                    "max_inflight": svc.max_inflight,
                    "total": svc.total,
                    "ok": svc.ok,
                    "failed": svc.failed,
                    "fallback": svc.fallback,
                    "rps": total_rps,
                })
            gw = self._rps_of(self._gateway_rps, now)
            gw5 = self._rps_of(self._gateway_rps, now, 5.0)
            return {
                "timestamp": round(time.time(), 3),
                "uptime": round(time.time() - (self.started_at or time.time()), 1),
                "traffic_rate": self.traffic_rate,
                "gateway": {
                    "total": self.gateway_total,
                    "ok": self.gateway_ok,
                    "failed": self.gateway_failed,
                    "rejected": self.gateway_rejected,
                    "fallback": self.gateway_fallback,
                    "inflight": self.gateway_inflight,
                    "rps": gw["total"],
                    "ok_rps": gw["ok"],
                    "failed_rps": gw["failed"],
                    "rejected_rps": gw["rejected"],
                    "rps5": gw5["total"],
                    "rate_limit": {
                        "rate": self.limiter.rate,
                        "burst": self.limiter.burst,
                        "tokens": round(self.limiter.tokens, 1),
                    },
                },
                "services": services_out,
                "nodes": nodes_out,
            }


# ----------------------------------------------------------------------
# default topology: gateway -> order(3 nodes) -> pay(3 nodes)
# ----------------------------------------------------------------------
DEFAULT_CONFIG = {
    "rate_limit": {"rate": 120.0, "burst": 60.0},
    "breaker": {
        "error_rate_threshold": 0.5,
        "min_samples": 4,
        "consecutive_failures": 3,
        "cooldown": 6.0,
        "half_open_probes": 2,
        "error_window": 10.0,
    },
    "services": [
        {
            "name": "order",
            "timeout": 0.4,
            "max_inflight": 30,
            "nodes": [
                {"id": "order-1", "host": "10.0.0.11:8080", "latency": 0.045, "weight": 10},
                {"id": "order-2", "host": "10.0.0.12:8080", "latency": 0.060, "weight": 8},
                {"id": "order-3", "host": "10.0.0.13:8080", "latency": 0.038, "weight": 12},
            ],
        },
        {
            "name": "pay",
            "timeout": 0.4,
            "max_inflight": 30,
            "nodes": [
                {"id": "pay-1", "host": "10.0.1.11:9090", "latency": 0.070, "weight": 10},
                {"id": "pay-2", "host": "10.0.1.12:9090", "latency": 0.055, "weight": 10},
                {"id": "pay-3", "host": "10.0.1.13:9090", "latency": 0.085, "weight": 7},
            ],
        },
    ],
}
