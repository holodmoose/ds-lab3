import time
from typing import Callable, Any, Optional
from collections import deque


class CircuitOpenException(Exception):
    """Raised when the circuit breaker is open and calls are blocked."""

    def __init__(self, service):
        self.service = service


class CircuitBreaker:
    def __init__(
        self,
        service: str,
        window_size: int = 10,
        failure_threshold: int = 5,
        recovery_timeout: int = 30,
    ):
        self.service = service
        self.window_size = window_size
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout

        self.state = "closed" 
        self.open_since = None

        self.results = deque(maxlen=window_size)

    def _failure_count(self) -> int:
        return self.results.count(False)

    def call(self, func: Callable, *args, **kwargs) -> Any:
        now = time.time()

        # ----- OPEN -----
        if self.state == "open":
            if now - self.open_since >= self.recovery_timeout:
                self.state = "half-open"
            else:
                raise CircuitOpenException(self.service)

        try:
            result = func(*args, **kwargs)
        except Exception:
            # record failure
            self.results.append(False)

            if self.state == "half-open":
                self.state = "open"
                self.open_since = time.time()
                raise CircuitOpenException(self.service)

            if self._failure_count() >= self.failure_threshold:
                self.state = "open"
                self.open_since = time.time()
                raise CircuitOpenException(self.service)

            raise CircuitOpenException(self.service)

        # ----- SUCCESS -----
        self.results.append(True)

        if self.state == "half-open":
            # successful probe closes the circuit
            self.state = "closed"
            self.results.clear()

        return result
