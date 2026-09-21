"""
test_mesh.py
============

零依赖测试套件 (unittest):
    - 断路器 CLOSED/OPEN/HALF_OPEN 三态状态机全部分支
    - 平滑加权负载均衡的比例与平滑性
    - 令牌桶突发/补充
    - 异步内核集成: 限流分流、故障熔断、冷却后半开自愈
    - 多线程并发访问下的线程安全

运行: python -m unittest -v test_mesh
"""

import asyncio
import threading
import time
import unittest

from mesh_kernel import (
    CLOSED, OPEN, HALF_OPEN,
    SUCCESS, TIMEOUT, ERROR,
    CircuitBreaker, MeshConfig, NodeSpec, SmoothWeightedLB, TokenBucket,
    TrafficKernel,
)


class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


# ---------------------------------------------------------------------------
# 断路器三态状态机
# ---------------------------------------------------------------------------


class BreakerTest(unittest.TestCase):
    def make(self, **overrides):
        cfg = MeshConfig(
            failure_threshold=3,
            error_rate_threshold=0.5,
            error_window_min=4,
            error_window_size=10,
            open_cooldown=5.0,
            half_open_success=2,
            half_open_max_trial=1,
        )
        for key, value in overrides.items():
            setattr(cfg, key, value)
        return CircuitBreaker("n1", cfg, clock=self.clock), cfg

    def setUp(self):
        self.clock = FakeClock()

    def test_stays_closed_on_success(self):
        br, _ = self.make()
        for _ in range(20):
            self.assertTrue(br.can_call())
            br.record_success()
        self.assertEqual(br.state, CLOSED)
        self.assertEqual(br.snapshot()["open_count"], 0)

    def test_opens_after_consecutive_failures(self):
        br, _ = self.make()
        br.record_failure()
        br.record_failure()
        self.assertEqual(br.state, CLOSED)
        br.record_failure()
        self.assertEqual(br.state, OPEN)
        # OPEN 期间全部快速失败
        self.assertFalse(br.can_call())
        snap = br.snapshot()
        self.assertAlmostEqual(snap["cooldown_remaining"], 5.0, delta=0.01)

    def test_error_rate_trip(self):
        # 2 成功 + 4 失败: 错误率 4/6 = 0.667 且连续失败未达阈值
        cfg = MeshConfig(failure_threshold=10, error_rate_threshold=0.5,
                         error_window_min=4, open_cooldown=5.0)
        br = CircuitBreaker("n2", cfg, clock=self.clock)
        outcomes = (SUCCESS, SUCCESS, ERROR, ERROR, ERROR, ERROR, ERROR)
        for index, outcome in enumerate(outcomes):
            if outcome == SUCCESS:
                br.record_success()
            else:
                still_closed_before = br.state == CLOSED
                br.record_failure(ERROR)
                # 前 4 次失败错误率 4/6 = 0.667 时即应触发熔断
                if still_closed_before and br.state == OPEN:
                    break
        self.assertEqual(br.state, OPEN)
        self.assertFalse(br.can_call())

    def test_cooldown_then_half_open_single_trial(self):
        br, _ = self.make()
        for _ in range(3):
            br.record_failure(TIMEOUT)
        self.assertEqual(br.state, OPEN)
        # 冷却未到: 拒绝
        self.clock.t += 4.0
        self.assertFalse(br.can_call())
        self.assertEqual(br.state, OPEN)
        # 冷却到: 进入半开, 仅放行 1 个探针
        self.clock.t += 1.1
        self.assertTrue(br.can_call())
        self.assertEqual(br.state, HALF_OPEN)
        self.assertFalse(br.can_call())  # 单飞, 第二个被挡

    def test_half_open_success_recovers(self):
        br, cfg = self.make()
        for _ in range(3):
            br.record_failure()
        self.clock.t += 5.1
        self.assertTrue(br.can_call())
        br.record_success()
        self.assertEqual(br.state, HALF_OPEN)
        # 第一个探针完成后, 才允许下一个探针
        self.assertTrue(br.can_call())
        br.record_success()
        self.assertEqual(br.state, CLOSED)
        self.assertEqual(br.consecutive_failures, 0)

    def test_half_open_failure_reopens_and_resets_cooldown(self):
        br, _ = self.make()
        for _ in range(3):
            br.record_failure()
        first_wake = br.wake_at
        self.clock.t += 5.1
        self.assertTrue(br.can_call())
        br.record_failure(TIMEOUT)
        self.assertEqual(br.state, OPEN)
        # 冷却窗口被重置 (从当前时刻重新计时)
        self.assertGreater(br.wake_at, first_wake)
        self.assertAlmostEqual(br.wake_at - self.clock.t, 5.0, delta=0.01)

    def test_release_trial_does_not_consume_slot(self):
        br, _ = self.make()
        for _ in range(3):
            br.record_failure()
        self.clock.t += 5.1
        self.assertTrue(br.can_call())
        self.assertFalse(br.can_call())
        br.release_trial()
        self.assertTrue(br.can_call())


# ---------------------------------------------------------------------------
# 平滑加权负载均衡
# ---------------------------------------------------------------------------


class LBTest(unittest.TestCase):
    def test_distribution_ratio(self):
        lb = SmoothWeightedLB()
        lb.update_weights({"a": 5, "b": 1, "c": 1})
        counts = {"a": 0, "b": 0, "c": 0}
        for _ in range(700):
            counts[lb.pick(["a", "b", "c"])] += 1
        self.assertAlmostEqual(counts["a"] / 700, 5 / 7, delta=0.02)
        self.assertAlmostEqual(counts["b"] / 700, 1 / 7, delta=0.02)

    def test_smooth_sequence_no_burst(self):
        # 5:1:1 时, 一个完整周期 7 次选择中 a 最多连续出现 2 次
        lb = SmoothWeightedLB()
        lb.update_weights({"a": 5, "b": 1, "c": 1})
        seq = [lb.pick(["a", "b", "c"]) for _ in range(7)]
        max_run = run = 1
        for prev, cur in zip(seq, seq[1:]):
            run = run + 1 if prev == cur else 1
            max_run = max(max_run, run)
        self.assertLessEqual(max_run, 2)
        self.assertEqual(sorted(seq), ["a", "a", "a", "a", "a", "b", "c"])

    def test_empty_eligible(self):
        lb = SmoothWeightedLB()
        lb.update_weights({"a": 1})
        self.assertIsNone(lb.pick(["x", "y"]))
        self.assertIsNone(lb.pick([]))


# ---------------------------------------------------------------------------
# 令牌桶
# ---------------------------------------------------------------------------


class TokenBucketTest(unittest.TestCase):
    def test_burst_then_refill(self):
        clock = FakeClock()
        bucket = TokenBucket(rate=10, capacity=5, clock=clock)
        for _ in range(5):
            self.assertTrue(bucket.try_acquire())
        self.assertFalse(bucket.try_acquire())
        clock.t += 0.1  # 补充 1 个令牌
        self.assertTrue(bucket.try_acquire())
        self.assertFalse(bucket.try_acquire())

# ---------------------------------------------------------------------------
# 异步内核集成
# ---------------------------------------------------------------------------


def fast_config(**overrides):
    cfg = MeshConfig(
        rate_limit=300,
        burst_capacity=60,
        max_concurrency=200,
        failure_threshold=3,
        error_rate_threshold=0.6,
        error_window_min=6,
        open_cooldown=1.5,
        half_open_success=2,
        half_open_max_trial=1,
        call_timeout=0.12,
        base_rps=0,
        weight_interval=0.5,
    )
    for key, value in overrides.items():
        setattr(cfg, key, value)
    return cfg


class KernelIntegrationTest(unittest.TestCase):
    def test_traffic_flows_and_burst_throttled(self):
        cfg = fast_config(rate_limit=200, burst_capacity=20)
        kernel = TrafficKernel(cfg)
        kernel.start()
        try:
            time.sleep(0.3)
            kernel.set_traffic(100)
            time.sleep(1.2)
            before = kernel.snapshot()["totals"]["throttled"]
            kernel.burst(duration=1.0, rps=900)
            time.sleep(1.4)
            snap = kernel.snapshot()
            self.assertGreater(snap["measured_rps"], 50)
            self.assertGreater(snap["totals"]["throttled"] - before, 10)
            # 五个节点都有流量命中
            self.assertTrue(all(n["success"] > 0 for n in snap["nodes"]))
        finally:
            kernel.stop()

    def test_fault_injection_trips_and_heals(self):
        cfg = fast_config(open_cooldown=1.0, half_open_success=2, call_timeout=0.12)
        kernel = TrafficKernel(cfg)
        events = []
        kernel.start()
        unsub = kernel.add_listener(lambda e: events.append(e) if e["kind"] == "breaker" else None)
        try:
            kernel.set_traffic(150)
            time.sleep(0.6)
            kernel.inject_fault("order-3", delay=0.3)
            # 等待熔断 OPEN
            deadline = time.time() + 3.0
            while time.time() < deadline:
                node = next(n for n in kernel.snapshot()["nodes"] if n["id"] == "order-3")
                if node["breaker"]["state"] == OPEN:
                    break
                time.sleep(0.05)
            node = next(n for n in kernel.snapshot()["nodes"] if n["id"] == "order-3")
            self.assertEqual(node["breaker"]["state"], OPEN)
            # OPEN 期间该节点吞吐近似为零 (流量切除); 等待 1 秒统计窗口翻篇
            time.sleep(1.3)
            node = next(n for n in kernel.snapshot()["nodes"] if n["id"] == "order-3")
            self.assertLessEqual(node["ok_rps"], 0)
            self.assertLessEqual(node["rps"], 0)
            self.assertGreaterEqual(node["breaker"]["open_count"], 1)

            # 保持故障: HALF_OPEN 探针失败 -> 重新 OPEN
            time.sleep(1.3)
            states_during_fault = set()
            deadline = time.time() + 2.5
            saw_reopen = False
            while time.time() < deadline:
                node = next(n for n in kernel.snapshot()["nodes"] if n["id"] == "order-3")
                states_during_fault.add(node["breaker"]["state"])
                if node["breaker"]["open_count"] >= 2 and node["breaker"]["state"] == OPEN:
                    saw_reopen = True
                time.sleep(0.03)
            self.assertIn(HALF_OPEN, states_during_fault)
            self.assertTrue(saw_reopen, "探针失败后应重新熔断并重置冷却")

            # 解除故障: 半开探针成功 -> CLOSED 自愈, 重新接流
            kernel.recover_node("order-3")
            deadline = time.time() + 4.0
            while time.time() < deadline:
                node = next(n for n in kernel.snapshot()["nodes"] if n["id"] == "order-3")
                if node["breaker"]["state"] == CLOSED and node["ok_rps"] > 0:
                    break
                time.sleep(0.05)
            node = next(n for n in kernel.snapshot()["nodes"] if n["id"] == "order-3")
            self.assertEqual(node["breaker"]["state"], CLOSED)
            self.assertGreater(node["ok_rps"], 0)

            kinds = [(e["data"]["state"], e["data"]["node"]) for e in events]
            self.assertTrue(any(s == OPEN and n == "order-3" for s, n in kinds))
            self.assertTrue(any(s == HALF_OPEN and n == "order-3" for s, n in kinds))
            self.assertTrue(any(s == CLOSED and n == "order-3" for s, n in kinds))
        finally:
            unsub()
            kernel.stop()

    def test_full_demo_runs_to_completion(self):
        cfg = fast_config(open_cooldown=1.2, call_timeout=0.12)
        kernel = TrafficKernel(cfg)
        kernel.start()
        seen = {"done": False}

        def listener(event):
            if event["kind"] == "demo" and event["data"].get("phase") == "full-demo-done":
                seen["done"] = True

        unsub = kernel.add_listener(listener)
        try:
            self.assertTrue(kernel.start_demo("full"))
            # 演示约 27 秒, 给足余量
            deadline = time.time() + 40.0
            while time.time() < deadline and not seen["done"]:
                time.sleep(0.1)
            self.assertTrue(seen["done"], "一键全流程演示应运行到完成")
        finally:
            unsub()
            kernel.stop()

    def test_second_demo_rejected_while_running(self):
        cfg = fast_config()
        kernel = TrafficKernel(cfg)
        kernel.start()
        try:
            self.assertTrue(kernel.start_demo("normal"))
            self.assertFalse(kernel.start_demo("circuit"))
        finally:
            kernel.stop()


# ---------------------------------------------------------------------------
# 线程安全
# ---------------------------------------------------------------------------


class ThreadSafetyTest(unittest.TestCase):
    def test_concurrent_snapshots_and_control(self):
        cfg = fast_config()
        kernel = TrafficKernel(cfg)
        kernel.start()
        stop = threading.Event()
        errors = []

        def reader():
            try:
                while not stop.is_set():
                    snap = kernel.snapshot()
                    _ = snap["nodes"][0]["breaker"]["state"]
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        def controller():
            try:
                i = 0
                while not stop.is_set():
                    kernel.set_traffic(50 + (i % 5) * 40)
                    if i % 4 == 0:
                        kernel.inject_fault("order-3", delay=0.2 if i % 8 == 0 else 0.0)
                    i += 1
                    time.sleep(0.01)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        def listener_spam():
            try:
                while not stop.is_set():
                    kernel.recent_events(0)
                    time.sleep(0.01)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [
            threading.Thread(target=reader),
            threading.Thread(target=reader),
            threading.Thread(target=controller),
            threading.Thread(target=listener_spam),
        ]
        try:
            kernel.set_traffic(120)
            for thread in threads:
                thread.start()
            time.sleep(2.0)
            stop.set()
            for thread in threads:
                thread.join(timeout=3)
            self.assertEqual(errors, [])
        finally:
            kernel.stop()


if __name__ == "__main__":
    unittest.main(verbosity=2)