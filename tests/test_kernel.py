"""End-to-end tests for the async mesh kernel.

The kernel is driven on a real background asyncio loop, but simulated
latencies are zeroed via injected delays so the suite stays fast.
"""

import asyncio
import threading
import time
import unittest

from service_mesh.breaker import CLOSED, OPEN, HALF_OPEN
from service_mesh.kernel import (
    MeshKernel,
    DEFAULT_CONFIG,
    REQUEST_OK,
    REQUEST_REJECTED,
)


FAST_CONFIG = {
    "rate_limit": {"rate": 1000.0, "burst": 1000.0, "max_wait": 0.0},
    "breaker": {
        "error_rate_threshold": 0.5,
        "min_samples": 4,
        "consecutive_failures": 3,
        "cooldown": 1.0,
        "half_open_probes": 2,
        "error_window": 10.0,
    },
    "services": [
        {
            "name": "order",
            "timeout": 0.3,
            "max_inflight": 1000,
            "nodes": [
                {"id": "order-1", "host": "h1", "latency": 0.004, "weight": 10},
                {"id": "order-2", "host": "h2", "latency": 0.008, "weight": 5},
                {"id": "order-3", "host": "h3", "latency": 0.016, "weight": 1},
            ],
        },
        {
            "name": "pay",
            "timeout": 0.3,
            "max_inflight": 1000,
            "nodes": [
                {"id": "pay-1", "host": "p1", "latency": 0.005, "weight": 10},
                {"id": "pay-2", "host": "p2", "latency": 0.005, "weight": 10},
            ],
        },
    ],
}


class KernelTest(unittest.TestCase):
    def setUp(self):
        self.kernel = MeshKernel(config=FAST_CONFIG)
        self.kernel.start()

    def tearDown(self):
        self.kernel.stop()

    def _run(self, coro):
        return self.kernel.submit(coro).result(timeout=10)

    def test_happy_path_request(self):
        result = self._run(self.kernel.handle_request())
        self.assertEqual(result["result"], REQUEST_OK)
        self.assertEqual(len(result["hops"]), 2)
        snap = self.kernel.snapshot()
        self.assertEqual(snap["gateway"]["ok"], 1)

    def test_smooth_weighted_distribution(self):
        async def fire():
            await asyncio.gather(
                *[self.kernel.handle_request() for _ in range(160)]
            )

        self._run(fire())
        svc = self.kernel.services["order"]
        counts = [n.total_requests for n in svc.nodes]
        # SWRR proportions ~ 10:5:1 -> roughly 100/50/10 over 160 selections.
        self.assertGreater(counts[0], counts[1])
        self.assertGreater(counts[1], counts[2])
        self.assertGreater(counts[0], sum(counts) * 0.45)
        self.assertLess(counts[2], sum(counts) * 0.2)
        # every node must be selected eventually
        self.assertTrue(all(c > 0 for c in counts))

    def test_latency_fault_trips_breaker_and_reroutes(self):
        target = self.kernel.nodes["pay-1"]
        self.kernel.inject_latency("pay-1", 1.0)  # > 300ms timeout

        async def fire(n):
            await asyncio.gather(
                *[self.kernel.handle_request() for _ in range(n)]
            )

        self._run(fire(12))
        self.assertIs(target.breaker.state, OPEN)
        # fast-fail counter is populated while open
        self._run(fire(20))
        self.assertGreater(target.breaker_rejected, 0)
        # healthy peer keeps serving the whole pay tier
        healthy = self.kernel.nodes["pay-2"]
        self.assertIs(healthy.breaker.state, CLOSED)
        self.assertGreater(healthy.total_requests, 15)

    def test_half_open_self_heal(self):
        target = self.kernel.nodes["pay-1"]
        self.kernel.inject_latency("pay-1", 1.0)

        async def fire(n):
            await asyncio.gather(
                *[self.kernel.handle_request() for _ in range(n)]
            )

        self._run(fire(8))
        self.assertIs(target.breaker.state, OPEN)
        # wait through cooldown, then clear the fault before probes land
        time.sleep(1.1)
        self.kernel.inject_latency("pay-1", 0.0)
        deadline = time.time() + 6
        while target.breaker.state != CLOSED and time.time() < deadline:
            self._run(fire(4))
            time.sleep(0.15)
        self.assertIs(target.breaker.state, CLOSED)

    def test_half_open_reopens_on_bad_probe(self):
        target = self.kernel.nodes["pay-1"]
        self.kernel.inject_latency("pay-1", 1.0)

        async def fire(n):
            await asyncio.gather(
                *[self.kernel.handle_request() for _ in range(n)]
            )

        self._run(fire(8))
        self.assertIs(target.breaker.state, OPEN)
        seen_half = False
        time.sleep(1.1)
        # fault still present -> probes fail -> breaker re-opens
        deadline = time.time() + 4
        while time.time() < deadline:
            self._run(fire(2))
            state = target.breaker.state
            if state == HALF_OPEN:
                seen_half = True
            if seen_half and state == OPEN:
                break
            time.sleep(0.1)
        self.assertTrue(seen_half)
        self.assertIs(target.breaker.state, OPEN)

    def test_breaker_probe_slot_released_on_routing_miss(self):
        # Two half-open nodes; routing admits a probe slot from each but
        # selects only one. The loser must release its slot.
        for node_id in ("pay-1", "pay-2"):
            node = self.kernel.nodes[node_id]
            node.breaker.force(HALF_OPEN)
        node, reason = self.kernel.route("pay")
        self.assertIsNone(reason)
        other = self.kernel.nodes["pay-2" if node.id == "pay-1" else "pay-1"]
        self.assertEqual(other.breaker.snapshot()["probes_out"], 0)

    def test_offline_node_excluded(self):
        self.kernel.set_offline("pay-1", True)
        for _ in range(20):
            node, reason = self.kernel.route("pay")
            self.assertEqual(node.id, "pay-2")

    def test_reset_restores_closed_state(self):
        self.kernel.inject_latency("pay-1", 1.0)

        async def fire():
            await asyncio.gather(
                *[self.kernel.handle_request() for _ in range(8)]
            )

        self._run(fire())
        self.assertIs(self.kernel.nodes["pay-1"].breaker.state, OPEN)
        self.kernel.reset()
        self.assertIs(self.kernel.nodes["pay-1"].breaker.state, CLOSED)
        snap = self.kernel.snapshot()
        self.assertEqual(snap["gateway"]["total"], 0)
        self.assertEqual(snap["nodes"][0]["injected_delay"], 0)


class RateLimitTest(unittest.TestCase):
    def test_gateway_sheds_over_limit(self):
        config = {
            "rate_limit": {"rate": 10.0, "burst": 5.0, "max_wait": 0.0},
            "breaker": DEFAULT_CONFIG["breaker"],
            "services": [
                {"name": "order", "timeout": 1.0,
                 "nodes": [{"id": "o1", "host": "h", "latency": 0.0}]},
                {"name": "pay", "timeout": 1.0,
                 "nodes": [{"id": "p1", "host": "h", "latency": 0.0}]},
            ],
        }
        kernel = MeshKernel(config=config)
        kernel.start()
        try:
            async def fire():
                return await asyncio.gather(
                    *[kernel.handle_request() for _ in range(40)]
                )

            results = kernel.submit(fire()).result(timeout=10)
            rejected = sum(1 for r in results if r["result"] == REQUEST_REJECTED)
            self.assertGreaterEqual(rejected, 20)
        finally:
            kernel.stop()


if __name__ == "__main__":
    unittest.main()
