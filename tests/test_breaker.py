"""Unit tests for the three-state circuit breaker state machine."""

import unittest

from service_mesh.breaker import CircuitBreaker, CLOSED, OPEN, HALF_OPEN


class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t

    def advance(self, seconds):
        self.t += seconds


class CircuitBreakerTest(unittest.TestCase):
    def make(self, **kwargs):
        self.clock = FakeClock()
        return CircuitBreaker(
            "n1",
            error_rate_threshold=0.5,
            min_samples=4,
            consecutive_failures=3,
            cooldown=5.0,
            half_open_probes=2,
            clock=self.clock,
        )

    def test_stays_closed_on_success(self):
        cb = self.make()
        for _ in range(10):
            self.assertTrue(cb.allow_request()[0])
            self.assertIsNone(cb.record_success())
        self.assertIs(cb.state, CLOSED)
        self.assertEqual(cb.snapshot()["failure_rate"], 0.0)

    def test_trips_on_consecutive_failures(self):
        cb = self.make()
        for _ in range(2):
            self.assertTrue(cb.allow_request()[0])
            self.assertIsNone(cb.record_failure())
        self.assertTrue(cb.allow_request()[0])
        transition = cb.record_failure()
        self.assertEqual(transition[0], "CLOSED->OPEN")
        self.assertIs(cb.state, OPEN)

    def test_trips_on_error_rate(self):
        cb = self.make()
        # 3 successes then failures: by sample #6 failure rate is 50%+
        results = ["ok", "ok", "ok", "fail", "fail"]
        for result in results:
            cb.allow_request()
            if result == "ok":
                cb.record_success()
            else:
                cb.record_failure()
        self.assertIs(cb.state, CLOSED)
        cb.allow_request()
        transition = cb.record_failure()
        self.assertEqual(transition[0], "CLOSED->OPEN")

    def test_open_fast_fails_and_counts(self):
        cb = self.make()
        for _ in range(3):
            cb.allow_request()
            cb.record_failure()
        self.assertIs(cb.state, OPEN)
        allowed, transition = cb.allow_request()
        self.assertFalse(allowed)
        self.assertIsNone(transition)
        self.assertEqual(cb.snapshot()["rejected"], 1)

    def test_half_open_probe_limit(self):
        cb = self.make()
        for _ in range(3):
            cb.allow_request()
            cb.record_failure()
        self.clock.advance(5.0)
        allowed, transition = cb.allow_request()
        self.assertTrue(allowed)
        self.assertEqual(transition[0], "OPEN->HALF_OPEN")
        self.assertIs(cb.state, HALF_OPEN)
        # the first transition already consumed one probe slot
        allowed2, _ = cb.allow_request()
        self.assertTrue(allowed2)
        allowed3, _ = cb.allow_request()
        self.assertFalse(allowed3)

    def test_probe_failure_reopens_and_resets_cooldown(self):
        cb = self.make()
        for _ in range(3):
            cb.allow_request()
            cb.record_failure()
        self.clock.advance(5.0)
        cb.allow_request()  # -> half open probe
        transition = cb.record_failure()
        self.assertEqual(transition[0], "CLOSED->OPEN")  # re-opened
        # cooldown was reset: immediate request must still be rejected
        allowed, _ = cb.allow_request()
        self.assertFalse(allowed)

    def test_probe_successes_close(self):
        cb = self.make()
        for _ in range(3):
            cb.allow_request()
            cb.record_failure()
        self.clock.advance(5.0)
        cb.allow_request()  # probe #1 slot
        self.assertIsNone(cb.record_success())
        self.assertIs(cb.state, HALF_OPEN)
        cb.allow_request()  # probe #2 slot
        transition = cb.record_success()
        self.assertEqual(transition[0], "HALF_OPEN->CLOSED")
        self.assertIs(cb.state, CLOSED)
        snap = cb.snapshot()
        self.assertEqual(snap["failure_rate"], 0.0)

    def test_error_window_pruning(self):
        cb = self.make()
        for _ in range(3):
            cb.allow_request()
            cb.record_failure()  # trips open
        self.assertIs(cb.state, OPEN)
        self.clock.advance(20.0)
        cb.allow_request()  # to half-open
        cb.allow_request()
        cb.record_success()
        cb.record_success()  # closes and clears window
        self.assertIs(cb.state, CLOSED)
        for _ in range(2):
            cb.allow_request()
            cb.record_failure()
        # old samples must not count: 2 failures alone should not re-trip
        self.assertIs(cb.state, CLOSED)


if __name__ == "__main__":
    unittest.main()
