"""Concurrency / deadlock / race stress tests and HTTP API smoke tests."""

import asyncio
import json
import threading
import time
import unittest
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from service_mesh.kernel import MeshKernel
from service_mesh.server import ConsoleServer


STRESS_CONFIG = {
    "rate_limit": {"rate": 500.0, "burst": 500.0, "max_wait": 0.0},
    "breaker": {
        "error_rate_threshold": 0.5, "min_samples": 4,
        "consecutive_failures": 3, "cooldown": 1.0,
        "half_open_probes": 2, "error_window": 10.0,
    },
    "services": [
        {"name": "order", "timeout": 0.3, "max_inflight": 1000,
         "nodes": [
             {"id": "order-1", "host": "h1", "latency": 0.0},
             {"id": "order-2", "host": "h2", "latency": 0.0},
         ]},
        {"name": "pay", "timeout": 0.3, "max_inflight": 1000,
         "nodes": [
             {"id": "pay-1", "host": "p1", "latency": 0.0},
             {"id": "pay-2", "host": "p2", "latency": 0.0},
         ]},
    ],
}


class ConcurrencyTest(unittest.TestCase):
    def test_mixed_threads_no_deadlock_or_race(self):
        kernel = MeshKernel(config=STRESS_CONFIG)
        kernel.start()
        stop = threading.Event()
        errors = []

        def request_worker():
            try:
                while not stop.is_set():
                    kernel.call("handle_request")
            except Exception as exc:
                errors.append(exc)

        def fault_worker():
            toggle = 0.0
            try:
                while not stop.is_set():
                    toggle = 1.0 if toggle == 0.0 else 0.0
                    kernel.inject_latency("pay-1", toggle)
                    time.sleep(0.25)
            except Exception as exc:
                errors.append(exc)

        def snapshot_worker():
            try:
                while not stop.is_set():
                    snap = kernel.snapshot()
                    json.dumps(snap)  # must always be serializable
                    kernel.events.poll(0)
                    time.sleep(0.02)
            except Exception as exc:
                errors.append(exc)

        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = [pool.submit(request_worker) for _ in range(6)]
            futures += [pool.submit(fault_worker), pool.submit(snapshot_worker)]
            time.sleep(6.0)
            stop.set()
            for future in futures:
                future.result(timeout=10)

        kernel.stop()
        self.assertEqual(errors, [])

        snap = kernel.snapshot()
        self.assertGreater(snap["gateway"]["total"], 100)


class ServerApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ConsoleServer(port=0)
        host, port = cls.server.start(open_browser=False)
        cls.base = "http://%s:%d" % ("127.0.0.1", port)

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def _get(self, path):
        with urllib.request.urlopen(self.base + path, timeout=5) as resp:
            raw = resp.read().decode("utf-8")
            ctype = resp.headers.get("Content-Type", "")
            if "application/json" in ctype:
                return resp.status, json.loads(raw)
            return resp.status, raw

    def _post(self, payload):
        req = urllib.request.Request(
            self.base + "/api/action",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def test_index_page(self):
        status, body = self._get("/")
        self.assertEqual(status, 200)
        self.assertIn("Service Mesh", body)

    def test_state_snapshot(self):
        status, body = self._get("/api/state")
        self.assertEqual(status, 200)
        self.assertIn("gateway", body["kernel"])
        self.assertEqual(len(body["kernel"]["nodes"]), 6)
        self.assertIn("demo", body)

    def test_action_and_event_stream(self):
        result = self._post({"action": "burst", "count": 10})
        self.assertTrue(result["ok"])
        status, body = self._get("/api/events?after=0&wait=0")
        self.assertEqual(status, 200)
        self.assertGreater(len(body["events"]), 0)

    def test_inject_and_reset(self):
        self._post({"action": "inject_latency", "node": "pay-1", "delay": 0.5})
        _, body = self._get("/api/state")
        node = next(n for n in body["kernel"]["nodes"] if n["id"] == "pay-1")
        self.assertEqual(node["injected_delay"], 500)
        self._post({"action": "reset"})
        _, body = self._get("/api/state")
        node = next(n for n in body["kernel"]["nodes"] if n["id"] == "pay-1")
        self.assertEqual(node["injected_delay"], 0)
        self.assertEqual(node["state"], "CLOSED")


if __name__ == "__main__":
    unittest.main()
