"""Thread-safe three-state circuit breaker state machine.

States
------
CLOSED      : requests flow normally; failures are counted on a sliding
              time window.  When the failure rate (with a minimum sample
              size) or the consecutive failure streak reaches the trip
              threshold, the breaker immediately opens.
OPEN        : every request is fast-fail rejected without touching the
              node (cascade-failure isolation).  After ``cooldown``
              seconds the next probe attempt transitions to HALF_OPEN.
HALF_OPEN   : only a small number of probe requests are let through.  A
              streak of successful probes closes the breaker; any probe
              failure re-opens it and resets the cooldown window.

A monotonic, injectable clock is supported so unit tests can exercise
state transitions deterministically without real sleeping.
"""

from collections import deque
from threading import RLock
import itertools

CLOSED = "CLOSED"
OPEN = "OPEN"
HALF_OPEN = "HALF_OPEN"

_EPHEMERAL = (OPEN, HALF_OPEN)
_FAILURE = "FAILURE"
_SUCCESS = "SUCCESS"


class CircuitBreaker:
    """A single circuit breaker protecting one mesh node."""

    def __init__(
        self,
        node_id,
        error_rate_threshold=0.5,
        min_samples=4,
        consecutive_failures=3,
        cooldown=5.0,
        half_open_probes=2,
        error_window=10.0,
        clock=None,
    ):
        self.node_id = node_id
        self.error_rate_threshold = float(error_rate_threshold)
        self.min_samples = int(min_samples)
        self.consecutive_failures = int(consecutive_failures)
        self.cooldown = float(cooldown)
        self.half_open_probes = int(half_open_probes)
        self.error_window = float(error_window)
        self._clock = clock

        self._lock = RLock()
        self._state = CLOSED
        self._opened_at = None
        self._probe_streak = 0
        self._probes_out = 0
        self._fail_streak = 0
        self._successes = 0
        self._failures = 0
        # sliding window of (timestamp, outcome)
        self._window = deque()
        self._transitions = 0
        self._rejected = 0
        self._ids = itertools.count(1)

    @property
    def state(self):
        return self._state

    # ------------------------------------------------------------------
    def _now(self):
        if self._clock is not None:
            return float(self._clock())
        import time

        return time.monotonic()

    def _prune(self, now):
        cutoff = now - self.error_window
        window = self._window
        while window and window[0][0] < cutoff:
            window.popleft()

    def _window_rate(self, now):
        self._prune(now)
        total = len(self._window)
        if total < self.min_samples:
            return 0.0, total
        fails = sum(1 for _, outcome in self._window if outcome == _FAILURE)
        return fails / total, total

    def _trip_open(self, now, reason):
        self._state = OPEN
        self._opened_at = now
        self._probe_streak = 0
        self._probes_out = 0
        self._transitions += 1
        return ("CLOSED->OPEN", reason)

    def snapshot(self):
        """Return a plain-dict immutable view of breaker statistics."""
        now = self._now()
        with self._lock:
            rate, samples = self._window_rate(now)
            remaining = None
            if self._state == OPEN and self._opened_at is not None:
                remaining = max(0.0, self.cooldown - (now - self._opened_at))
            return {
                "node_id": self.node_id,
                "state": self._state,
                "failure_rate": round(rate, 3),
                "window_samples": samples,
                "fail_streak": self._fail_streak,
                "probe_streak": self._probe_streak,
                "probes_out": self._probes_out,
                "cooldown_remaining": (
                    round(remaining, 2) if remaining is not None else None
                ),
                "total_successes": self._successes,
                "total_failures": self._failures,
                "transitions": self._transitions,
                "rejected": self._rejected,
            }

    # ------------------------------------------------------------------
    def allow_request(self):
        """Decide whether a request may touch the protected node.

        Returns ``(allowed, transition)`` where ``transition`` is either
        None or ``("OPEN->HALF_OPEN", cooldown_elapsed)`` for an event log.
        """
        now = self._now()
        with self._lock:
            if self._state == CLOSED:
                return True, None
            if self._state == OPEN:
                elapsed = now - (self._opened_at if self._opened_at is not None else now)
                if elapsed >= self.cooldown:
                    self._state = HALF_OPEN
                    self._probe_streak = 0
                    self._probes_out = 0
                    self._transitions += 1
                    self._probes_out = 1
                    return True, ("OPEN->HALF_OPEN", round(elapsed, 3))
                self._rejected += 1
                return False, None
            # HALF_OPEN: admit only a bounded number of concurrent probes
            if self._probes_out < self.half_open_probes:
                self._probes_out += 1
                return True, None
            self._rejected += 1
            return False, None

    def release_probe(self):
        """Give back an unused half-open probe slot (routing miss)."""
        with self._lock:
            if self._state == HALF_OPEN and self._probes_out > 0:
                self._probes_out -= 1

    # ------------------------------------------------------------------
    def record_success(self):
        """Report a completed call as successful; return state transition info."""
        now = self._now()
        with self._lock:
            self._successes += 1
            self._fail_streak = 0
            self._window.append((now, _SUCCESS))
            self._prune(now)
            if self._state == HALF_OPEN:
                self._probes_out = max(0, self._probes_out - 1)
                self._probe_streak += 1
                if self._probe_streak >= self.half_open_probes:
                    self._state = CLOSED
                    self._opened_at = None
                    self._probe_streak = 0
                    self._probes_out = 0
                    self._window.clear()
                    self._transitions += 1
                    return ("HALF_OPEN->CLOSED", None)
            return None

    def record_failure(self, reason="error"):
        """Report a failed call; return a transition tuple if the breaker tripped."""
        now = self._now()
        with self._lock:
            self._failures += 1
            self._fail_streak += 1
            self._window.append((now, _FAILURE))
            rate, samples = self._window_rate(now)

            if self._state == HALF_OPEN:
                self._probes_out = max(0, self._probes_out - 1)
                return self._trip_open(now, "probe failure")

            if self._state == CLOSED:
                if self._fail_streak >= self.consecutive_failures:
                    return self._trip_open(
                        now,
                        "consecutive failures %d" % self._fail_streak,
                    )
                if samples >= self.min_samples and rate >= self.error_rate_threshold:
                    return self._trip_open(
                        now,
                        "error rate %.0f%% over %d samples" % (rate * 100, samples),
                    )
            return None

    # ------------------------------------------------------------------
    def force(self, state):
        """Administratively force a state (used by reset / tests)."""
        with self._lock:
            self._state = state
            if state in _EPHEMERAL:
                self._opened_at = self._now()
            else:
                self._opened_at = None
            self._probe_streak = 0
            self._probes_out = 0
            self._fail_streak = 0
            self._window.clear()

    def reset(self):
        with self._lock:
            self._state = CLOSED
            self._opened_at = None
            self._probe_streak = 0
            self._probes_out = 0
            self._fail_streak = 0
            self._window.clear()
