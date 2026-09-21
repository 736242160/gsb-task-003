"""Pure-stdlib service mesh simulation kernel.

Modules:
    breaker      - three-state circuit breaker state machine
    events       - thread-safe bounded time-series event log
    kernel       - async mesh simulation (load balancing, rate limit, routing)
    demo         - scripted traffic / fault-injection demonstration scenarios
    server       - built-in http.server based visualization console
"""

__all__ = ["breaker", "events", "kernel", "demo", "server"]
__version__ = "1.0.0"
