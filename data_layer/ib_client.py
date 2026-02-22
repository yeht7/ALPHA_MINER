"""IB Gateway connection manager with pacing / rate-limit control."""

from __future__ import annotations

import asyncio
import logging
import os
import time

from ib_async import IB

logger = logging.getLogger(__name__)

# IBKR pacing: ≤50 identical-type historical-data requests per 10 min window
_PACING_WINDOW = 600  # seconds
_PACING_MAX_REQUESTS = 50
_MIN_REQUEST_INTERVAL = 1.0  # floor between any two requests


class PacingLimiter:
    """Token-bucket style limiter that respects IBKR pacing rules."""

    def __init__(
        self,
        max_requests: int = _PACING_MAX_REQUESTS,
        window: float = _PACING_WINDOW,
        min_interval: float = _MIN_REQUEST_INTERVAL,
    ):
        self._max = max_requests
        self._window = window
        self._min_interval = min_interval
        self._timestamps: list[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            # purge stale timestamps
            self._timestamps = [t for t in self._timestamps if now - t < self._window]

            if len(self._timestamps) >= self._max:
                wait = self._window - (now - self._timestamps[0])
                logger.warning("Pacing limit hit – sleeping %.1fs", wait)
                await asyncio.sleep(wait)
                now = time.monotonic()
                self._timestamps = [
                    t for t in self._timestamps if now - t < self._window
                ]

            if self._timestamps:
                elapsed = now - self._timestamps[-1]
                if elapsed < self._min_interval:
                    await asyncio.sleep(self._min_interval - elapsed)

            self._timestamps.append(time.monotonic())


class IBClientManager:
    """Manages a single IB Gateway connection with auto-retry."""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        client_id: int | None = None,
        max_retries: int = 5,
        retry_delay: float = 3.0,
    ):
        self.host = host or os.getenv("IB_GATEWAY_HOST", "host.docker.internal")
        self.port = port or int(os.getenv("IB_GATEWAY_PORT", "4002"))
        self.client_id = client_id or int(os.getenv("IB_CLIENT_ID", "1"))
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self.ib = IB()
        self.pacing = PacingLimiter()

    async def connect(self) -> IB:
        for attempt in range(1, self._max_retries + 1):
            try:
                await self.ib.connectAsync(
                    self.host, self.port, clientId=self.client_id
                )
                logger.info(
                    "Connected to IB Gateway %s:%s (attempt %d)",
                    self.host,
                    self.port,
                    attempt,
                )
                return self.ib
            except (ConnectionRefusedError, OSError, asyncio.TimeoutError) as exc:
                logger.warning(
                    "Connection attempt %d/%d failed: %s",
                    attempt,
                    self._max_retries,
                    exc,
                )
                if attempt == self._max_retries:
                    raise ConnectionError(
                        f"Cannot reach IB Gateway after {self._max_retries} attempts"
                    ) from exc
                await asyncio.sleep(self._retry_delay * attempt)
        raise ConnectionError("Unreachable")  # guard

    async def disconnect(self) -> None:
        if self.ib.isConnected():
            self.ib.disconnect()
            logger.info("Disconnected from IB Gateway")

    async def __aenter__(self) -> "IBClientManager":
        await self.connect()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.disconnect()
