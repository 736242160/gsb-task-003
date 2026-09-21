"""Scripted demonstration scenarios orchestrated on the simulation kernel.

Scenarios run in a background daemon thread driving kernel coroutines over
an event timeline (0.2s ticks), so the web UI animates topology changes in
real time.  An epoch token guards against overlapping runs.
"""

import threading
import time


class DemoRunner:
    def __init__(self, kernel):
        self.kernel = kernel
        self._thread = None
        self._lock = threading.RLock()
        self._epoch = 0
        self.active = None  # "burst" | "breaker"
        self.started_at = None
        self.elapsed = 0.0
        self.message = "idle"

    # ------------------------------------------------------------------
    def status(self):
        with self._lock:
            return {
                "active": self.active,
                "elapsed": round(self.elapsed, 1),
                "message": self.message,
            }

    def is_busy(self):
        with self._lock:
            return self._thread is not None and self._thread.is_alive()

    def cancel(self):
        with self._lock:
            self._epoch += 1
            self.kernel.set_traffic(0)
            self.active = None
            self.message = "demo cancelled"

    def start(self, scenario):
        if scenario not in ("burst", "breaker"):
            raise ValueError("unknown scenario: %r" % scenario)
        if self.is_busy():
            raise RuntimeError("another demo is already running")
        with self._lock:
            self._epoch += 1
            epoch = self._epoch
            self.active = scenario
            self.started_at = time.time()
            self.elapsed = 0.0
            self.message = "starting"
        self._thread = threading.Thread(
            target=self._run, args=(scenario, epoch), name="demo-%s" % scenario,
            daemon=True,
        )
        self._thread.start()

    # ------------------------------------------------------------------
    def _sleep(self, epoch, seconds):
        end = time.time() + seconds
        while time.time() < end:
            if self._epoch != epoch:
                return False
            with self._lock:
                self.elapsed = time.time() - self.started_at
            time.sleep(0.1)
        return self._epoch == epoch

    def _finish(self, epoch, ok=True, msg=""):
        with self._lock:
            if self._epoch == epoch:
                self.active = None
                self.elapsed = time.time() - self.started_at
                self.message = msg or ("completed" if ok else "aborted")

    # ------------------------------------------------------------------
    def _run(self, scenario, epoch):
        try:
            if scenario == "burst":
                self._run_burst(epoch)
            else:
                self._run_breaker(epoch)
        except Exception as exc:  # never let the demo thread die silently
            self.kernel.events.emit("ERROR", "demo", "demo error: %s" % exc)
            self._finish(epoch, ok=False, msg="error: %s" % exc)

    # ------------------------------------------------------------------
    def _run_burst(self, epoch):
        k = self.kernel
        k.events.emit("INFO", "demo", "=== DEMO 1: smooth burst load balancing ===")
        self._set(epoch, "baseline traffic 20 rps")
        k.set_traffic(20)
        if not self._sleep(epoch, 4.0):
            return self._finish(epoch, False, "cancelled")

        self._set(epoch, "ramping to 60 rps (over gateway rate limit 55/s)")
        k.set_traffic(60)
        if not self._sleep(epoch, 4.0):
            return self._finish(epoch, False, "cancelled")

        self._set(epoch, "firing 140-request concurrent burst")
        k.burst(140)
        if not self._sleep(epoch, 3.0):
            return self._finish(epoch, False, "cancelled")

        self._set(epoch, "settling back to 20 rps")
        k.set_traffic(20)
        if not self._sleep(epoch, 3.0):
            return self._finish(epoch, False, "cancelled")

        with k._lock:
            order = k.services["order"]
            loads = sorted(
                ((n.id, n.total_requests, n.ewma_latency) for n in order.nodes),
                key=lambda item: -item[1],
            )
        k.set_traffic(0)
        k.events.emit(
            "OK", "demo",
            "DEMO 1 done - order tier distribution: "
            + ", ".join("%s=%d (%.0fms)" % (i, c, l * 1000) for i, c, l in loads)
            + " (SWRR smooth weights, excess burst shed at gateway)",
        )
        self._finish(epoch, True, "burst demo completed")

    # ------------------------------------------------------------------
    def _run_breaker(self, epoch):
        k = self.kernel
        target = "pay-1"
        k.events.emit("INFO", "demo", "=== DEMO 2: latency fault -> trip -> half-open self-heal ===")

        self._set(epoch, "healthy baseline traffic 30 rps")
        k.set_traffic(30)
        if not self._sleep(epoch, 4.0):
            return self._finish(epoch, False, "cancelled")

        self._set(epoch, "injecting +650ms latency into pay-1 (timeout=400ms)")
        k.inject_latency(target, 0.65)
        if not self._sleep(epoch, 3.0):
            return self._finish(epoch, False, "cancelled")

        with k._lock:
            state = k.nodes[target].breaker.state
            health = k.nodes[target].health
        self._set(epoch, "pay-1 breaker=%s health=%.0f%%; traffic should reroute" % (state, health))
        if not self._sleep(epoch, 4.0):
            return self._finish(epoch, False, "cancelled")

        with k._lock:
            state = k.nodes[target].breaker.state
            cooldown = k.nodes[target].breaker.snapshot()["cooldown_remaining"]
        self._set(epoch, "pay-1=%s cooldown_remaining=%s; waiting for half-open probes" % (state, cooldown))
        if not self._sleep(epoch, 9.0):
            return self._finish(epoch, False, "cancelled")

        self._set(epoch, "clearing latency fault on pay-1")
        k.inject_latency(target, 0.0)
        if not self._sleep(epoch, 8.0):
            return self._finish(epoch, False, "cancelled")

        with k._lock:
            final_state = k.nodes[target].breaker.state
            final_health = k.nodes[target].health
        k.set_traffic(0)
        if final_state == "CLOSED":
            k.events.emit(
                "OK", "demo",
                "DEMO 2 done - pay-1 self-healed: breaker CLOSED, health back to %.0f%%"
                % final_health,
            )
            self._finish(epoch, True, "breaker demo completed (self-healed)")
        else:
            k.events.emit(
                "WARN", "demo",
                "DEMO 2 ended with pay-1 in %s state (health %.0f%%)"
                % (final_state, final_health),
            )
            self._finish(epoch, True, "breaker demo completed (%s)" % final_state)

    def _set(self, epoch, message):
        with self._lock:
            if self._epoch != epoch:
                return False
            self.message = message
            self.elapsed = time.time() - self.started_at
        self.kernel.events.emit("INFO", "demo", message)
        return True
