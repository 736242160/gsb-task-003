"""Thread-safe bounded event log with long-poll notification."""

from collections import deque
from threading import Condition

LEVELS = ("DEBUG", "INFO", "OK", "WARN", "ERROR", "REJECT")


class EventLog:
    """Append-only ring buffer of governance events.

    Each event is a dict::

        {"seq": int, "ts": float, "level": str, "category": str,
         "message": str, "node": str|None, "service": str|None}

    Consumers (the web console) long-poll on :meth:`wait` and fetch
    everything after a given sequence number.
    """

    def __init__(self, capacity=2000, clock=None):
        self._capacity = capacity
        self._clock = clock
        self._cond = Condition()
        self._events = deque()
        self._seq = 0

    def _now(self):
        if self._clock is not None:
            return float(self._clock())
        import time

        return time.time()

    def emit(self, level, category, message, node=None, service=None):
        level = level.upper()
        if level not in LEVELS:
            level = "INFO"
        with self._cond:
            self._seq += 1
            event = {
                "seq": self._seq,
                "ts": round(self._now(), 3),
                "level": level,
                "category": category,
                "message": message,
                "node": node,
                "service": service,
            }
            self._events.append(event)
            while len(self._events) > self._capacity:
                self._events.popleft()
            self._cond.notify_all()
        return event

    def wait(self, after_seq, timeout=5.0):
        """Block until an event with ``seq > after_seq`` exists (or timeout)."""
        with self._cond:
            if self._seq <= after_seq:
                self._cond.wait(timeout)
            return self._seq

    def poll(self, after_seq=0, limit=500):
        with self._cond:
            latest = self._seq
            if not self._events:
                return [], latest
            # cheap scan: buffer is small and bounded
            result = [e for e in self._events if e["seq"] > after_seq]
            if len(result) > limit:
                result = result[-limit:]
            return result, latest

    def clear(self):
        with self._cond:
            self._events.clear()
            self._seq = 0
            self._cond.notify_all()
