#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
 @ Date   : 2026/1/21
 @ Author : Administrator
 @ Description :
"""
import ast
import asyncio
import itertools
import json
import platform
import sys
import time
import warnings
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import websockets

from MT.Flopotech.BaseMethod.log_module import logger
from MT.Flopotech.Config.More_Account import WEBSOCKET_PRIVATE_CONFIG
from MT.Flopotech.BaseMethod.operate_config import OperateConfig

warnings.filterwarnings("ignore", category=DeprecationWarning)


def setup_event_loop_policy() -> None:
    """Call before asyncio.run (Windows compatibility)."""
    if platform.system() == "Windows" and sys.version_info >= (3, 8):
        force_selector = bool(WEBSOCKET_PRIVATE_CONFIG.get("force_selector_loop", False))
        if force_selector:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            logger.log_out(
                "warning",
                "force_selector_loop enabled; select() has fd limits on Windows.",
            )
        else:
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    warnings.filterwarnings("ignore", category=ResourceWarning)
    warnings.filterwarnings("ignore", category=RuntimeWarning)


def _to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_tokens(tokens_obj: Any) -> List[Tuple[str, str]]:
    """
    Accept multiple token formats:
    - [(account, token), ...]
    - {"account": "token", ...}
    - (("account", "token"), ...)
    Return List[(account, token)].
    """
    if tokens_obj is None:
        return []
    if isinstance(tokens_obj, dict):
        return [(str(k), str(v)) for k, v in tokens_obj.items()]
    if isinstance(tokens_obj, (list, tuple)):
        out: List[Tuple[str, str]] = []
        for item in tokens_obj:
            if isinstance(item, (list, tuple)) and len(item) == 2:
                out.append((str(item[0]), str(item[1])))
            else:
                raise ValueError(f"Invalid tokens entry: {item!r}")
        return out
    raise ValueError(f"Unsupported tokens type: {type(tokens_obj)!r}")


@dataclass
class ConnectionInfo:
    account_name: str
    headers: Dict[str, str]
    websocket: Optional[websockets.WebSocketClientProtocol] = None


class SendStats:
    def __init__(self, duration: int, rate: int) -> None:
        self.duration = duration
        self.rate = rate
        self.start_ts = time.perf_counter()
        self.success_per_sec = [0] * duration
        self.fail_per_sec = [0] * duration
        self.total_success = 0
        self.total_fail = 0
        self.late_success = 0
        self.late_fail = 0
        self.lock = asyncio.Lock()

    async def record(self, ok: bool, timestamp: float) -> None:
        idx = int(timestamp - self.start_ts)
        async with self.lock:
            if ok:
                self.total_success += 1
                if 0 <= idx < self.duration:
                    self.success_per_sec[idx] += 1
                else:
                    self.late_success += 1
            else:
                self.total_fail += 1
                if 0 <= idx < self.duration:
                    self.fail_per_sec[idx] += 1
                else:
                    self.late_fail += 1


class MarketOrder:
    """Market order open."""

    def __init__(self) -> None:
        self.config = WEBSOCKET_PRIVATE_CONFIG

        self.batch_size = _to_int(self.config.get("batch_size", 1000), 1000)
        self.client_ids = list(self.config.get("client_ids", []))
        self.client_id_cycle = itertools.cycle(self.client_ids)
        self.accounts_headers: List[Tuple[str, Dict[str, str]]] = []

        self.target_rate = max(1, _to_int(self.config.get("target_rate", 1000), 1000))
        self.target_duration = max(1, _to_int(self.config.get("target_duration", 10), 10))
        self.connections_per_account = max(
            1, _to_int(self.config.get("connections_per_account", 1), 1)
        )
        max_connections = _to_int(self.config.get("max_connections", self.target_rate), self.target_rate)
        self.max_connections = max(1, max_connections)

        self.order_data = {
            "eventType": "marketOrder",
            "eventData": {
                "action": "open",
                "orderFrom": "SELF",
                "symbolCode": "EURUSD",
                "side": "1",
                "leverage": "100",
                "price": "1.19000",
                "openOrderAmt": "100",
            },
        }

        self._payload = json.dumps(self.order_data, ensure_ascii=False)

        max_concurrency = _to_int(self.config.get("max_concurrency", self.batch_size), self.batch_size)
        self._max_concurrency = max(1, max_concurrency)
        self._connect_semaphore = asyncio.Semaphore(self._max_concurrency)

        self._max_retries = max(1, _to_int(self.config.get("max_retries", 2), 2))
        self._retry_delay = max(0.0, _to_float(self.config.get("retry_delay", 0.5), 0.5))
        self._open_timeout = max(0.1, _to_float(self.config.get("open_timeout", 3.0), 3.0))
        self._close_wait_timeout = max(1.0, _to_float(self.config.get("close_wait_timeout", 5.0), 5.0))

        self._connections: List[ConnectionInfo] = []
        self._sender_tasks: List[asyncio.Task] = []
        self._producer_task: Optional[asyncio.Task] = None

    async def generate_header(self) -> List[Tuple[str, Dict[str, str]]]:
        """Build headers for each account."""
        try:
            tokens_str = OperateConfig().get_ini_value("PARAMS", "tokens")
            if not tokens_str:
                logger.log_out("error", "tokens config is empty")
                return []

            try:
                tokens_obj = ast.literal_eval(tokens_str)
            except (ValueError, SyntaxError) as exc:
                logger.log_out("error", f"tokens config invalid: {exc}")
                return []

            pairs = _normalize_tokens(tokens_obj)
            if not pairs:
                logger.log_out("error", "tokens config produced no entries")
                return []
            if not self.client_ids:
                logger.log_out("error", "client_ids is empty")
                return []

            accounts_headers: List[Tuple[str, Dict[str, str]]] = []
            next_client_id = self.client_id_cycle.__next__
            for account, token in pairs:
                headers = {
                    "Access-Token": token,
                    "Client-ID": next_client_id(),
                }
                accounts_headers.append((str(account), headers))

            self.accounts_headers = accounts_headers
            return accounts_headers
        except KeyboardInterrupt:
            logger.log_out("info", "User interrupted")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.log_out("error", f"Failed to build headers: {exc}")
        return []

    async def receive_websocket_messages(
        self, websocket: websockets.WebSocketClientProtocol, account_name: str, timeout_duration: float
    ) -> int:
        """Receive WebSocket messages."""
        if timeout_duration <= 0:
            return 0

        message_count = 0
        try:
            recv = websocket.recv
            log_out = logger.log_out
            loads = json.loads

            while True:
                try:
                    message = await asyncio.wait_for(recv(), timeout=timeout_duration)
                    try:
                        message_data = loads(message)
                    except json.JSONDecodeError:
                        log_out("warning", f"Account {account_name} - message is not JSON: {message!r}")
                        continue
                    message_count += 1
                    log_out("debug", f"Account {account_name} - message: {message_data}")
                except asyncio.TimeoutError:
                    log_out("info", f"Account {account_name} - receive done, total {message_count}")
                    break
        except asyncio.CancelledError:
            logger.log_out("warning", f"Account {account_name} - receive cancelled")
            raise
        finally:
            if websocket and not websocket.closed:
                try:
                    await websocket.close()
                    logger.log_out("info", f"Account {account_name} - WebSocket closed")
                except Exception as exc:
                    logger.log_out("warning", f"Account {account_name} - close error: {exc}")
        return message_count

    def _build_connection_specs(self, headers_list: List[Tuple[str, Dict[str, str]]]) -> List[ConnectionInfo]:
        specs: List[ConnectionInfo] = []
        for account_name, headers in headers_list:
            for _ in range(self.connections_per_account):
                specs.append(ConnectionInfo(account_name=account_name, headers=headers))
        if len(specs) > self.max_connections:
            logger.log_out(
                "warning",
                f"Connections limited to {self.max_connections} (requested {len(specs)})",
            )
            specs = specs[: self.max_connections]
        return specs

    async def _connect_once(self, account_name: str, headers: Dict[str, str]) -> Optional[websockets.WebSocketClientProtocol]:
        url = self.config.get("websocket_url")
        if not url:
            logger.log_out("error", "websocket_url is missing")
            return None

        for attempt in range(1, self._max_retries + 1):
            try:
                websocket = await websockets.connect(
                    url,
                    extra_headers=headers,
                    close_timeout=10,
                    ping_interval=30,
                    ping_timeout=10,
                    open_timeout=self._open_timeout,
                )
                logger.log_out("info", f"Account {account_name} - WebSocket connected")
                return websocket
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.log_out(
                    "error",
                    f"Account {account_name} - connect error (attempt {attempt}): {exc}",
                )
                if attempt < self._max_retries and self._retry_delay > 0:
                    await asyncio.sleep(self._retry_delay * attempt)
        return None

    async def _connect_with_semaphore(self, info: ConnectionInfo) -> Optional[ConnectionInfo]:
        async with self._connect_semaphore:
            websocket = await self._connect_once(info.account_name, info.headers)
        if websocket is None:
            return None
        info.websocket = websocket
        return info

    async def _open_connections(self, headers_list: List[Tuple[str, Dict[str, str]]]) -> List[ConnectionInfo]:
        specs = self._build_connection_specs(headers_list)
        if not specs:
            return []
        tasks = [asyncio.create_task(self._connect_with_semaphore(info)) for info in specs]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        connections: List[ConnectionInfo] = []
        for result in results:
            if isinstance(result, ConnectionInfo):
                connections.append(result)
            elif isinstance(result, Exception):
                logger.log_out("error", f"Connection task failed: {result}")
        return connections

    async def _safe_close(self, websocket: Optional[websockets.WebSocketClientProtocol]) -> None:
        if websocket and not websocket.closed:
            try:
                await websocket.close()
            except Exception as exc:
                logger.log_out("warning", f"WebSocket close error: {exc}")

    async def _send_payload(self, info: ConnectionInfo, stats: SendStats) -> None:
        for attempt in range(1, self._max_retries + 1):
            try:
                if info.websocket is None or info.websocket.closed:
                    info.websocket = await self._connect_once(info.account_name, info.headers)
                    if info.websocket is None:
                        raise RuntimeError("Reconnect failed")

                await info.websocket.send(self._payload)
                await stats.record(True, time.perf_counter())
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if attempt >= self._max_retries:
                    logger.log_out(
                        "error",
                        f"Account {info.account_name} - send failed after retries: {exc}",
                    )
                    await stats.record(False, time.perf_counter())
                    return
                await self._safe_close(info.websocket)
                info.websocket = None
                if self._retry_delay > 0:
                    await asyncio.sleep(self._retry_delay * attempt)

    async def _sender_worker(self, info: ConnectionInfo, queue: asyncio.Queue, stats: SendStats) -> None:
        try:
            while True:
                item = await queue.get()
                if item is None:
                    queue.task_done()
                    break
                try:
                    await self._send_payload(info, stats)
                finally:
                    queue.task_done()
        except asyncio.CancelledError:
            raise

    async def _send_producer(self, queue: asyncio.Queue, rate: int, duration: int, start_ts: float) -> None:
        for second in range(duration):
            target_time = start_ts + second + 1
            for _ in range(rate):
                queue.put_nowait(1)
            sleep_for = target_time - time.perf_counter()
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)

    def _log_send_stats(self, stats: SendStats) -> None:
        for idx in range(stats.duration):
            success = stats.success_per_sec[idx]
            fail = stats.fail_per_sec[idx]
            if success < stats.rate:
                logger.log_out(
                    "warning",
                    f"Second {idx + 1}: success {success}/{stats.rate}, fail {fail}",
                )
            else:
                logger.log_out(
                    "info",
                    f"Second {idx + 1}: success {success}/{stats.rate}, fail {fail}",
                )
        logger.log_out(
            "info",
            f"Total success={stats.total_success}, fail={stats.total_fail}, "
            f"late_success={stats.late_success}, late_fail={stats.late_fail}",
        )

    async def _wait_for_responses(self, connections: List[ConnectionInfo]) -> None:
        receive_timeout = max(0.0, _to_float(self.config.get("receive_timeout", 5), 5.0))
        if receive_timeout <= 0:
            logger.log_out("warning", "receive_timeout <= 0, skip waiting for responses")
            return

        tasks = []
        for info in connections:
            if info.websocket and not info.websocket.closed:
                tasks.append(
                    asyncio.create_task(
                        self.receive_websocket_messages(
                            info.websocket, info.account_name, receive_timeout
                        )
                    )
                )
        if not tasks:
            logger.log_out("warning", "No active connections to receive responses")
            return

        results = await asyncio.gather(*tasks, return_exceptions=True)
        total_messages = 0
        for result in results:
            if isinstance(result, int):
                total_messages += result
            elif isinstance(result, Exception):
                logger.log_out("warning", f"Receive task error: {result}")
        logger.log_out("info", f"Total received messages: {total_messages}")

    async def send_subscribe_request(self) -> None:
        """Send 1000 messages per second for 10 seconds, then receive responses."""
        headers_list = await self.generate_header()
        if not headers_list:
            logger.log_out("error", "No headers available, skip trading requests")
            return

        connections = await self._open_connections(headers_list)
        if not connections:
            logger.log_out("error", "No WebSocket connections available")
            return

        self._connections = connections
        logger.log_out(
            "info",
            f"Send plan: {self.target_rate}/s for {self.target_duration}s, "
            f"connections={len(connections)}",
        )
        stats = SendStats(duration=self.target_duration, rate=self.target_rate)
        send_queue: asyncio.Queue = asyncio.Queue()
        self._sender_tasks = [
            asyncio.create_task(self._sender_worker(info, send_queue, stats))
            for info in connections
        ]

        try:
            self._producer_task = asyncio.create_task(
                self._send_producer(send_queue, self.target_rate, self.target_duration, stats.start_ts)
            )
            await self._producer_task
            await send_queue.join()

            for _ in self._sender_tasks:
                send_queue.put_nowait(None)
            await asyncio.gather(*self._sender_tasks, return_exceptions=True)
            self._sender_tasks.clear()
            self._producer_task = None

            self._log_send_stats(stats)
            await self._wait_for_responses(connections)
        except asyncio.CancelledError:
            if self._producer_task:
                self._producer_task.cancel()
            for task in self._sender_tasks:
                task.cancel()
            await asyncio.gather(*self._sender_tasks, return_exceptions=True)
            self._sender_tasks.clear()
            raise

        logger.log_out("info", "All account trading requests finished")

    async def close(self) -> None:
        """Cleanup resources."""
        if self._producer_task and not self._producer_task.done():
            self._producer_task.cancel()
        for task in self._sender_tasks:
            if not task.done():
                task.cancel()
        if self._sender_tasks:
            await asyncio.gather(*self._sender_tasks, return_exceptions=True)
        self._sender_tasks.clear()

        for info in list(self._connections):
            await self._safe_close(info.websocket)
        self._connections.clear()

        await asyncio.sleep(0)
        logger.log_out("info", "Cleanup completed")


async def main() -> None:
    main_start_time = time.time()
    market_order = MarketOrder()
    try:
        logger.log_out("info", "Start sending WebSocket requests...")
        await market_order.send_subscribe_request()
    finally:
        await market_order.close()

    main_end_time = time.time()
    logger.log_out("info", f"Done. Total time: {main_end_time - main_start_time:.4f}s")


if __name__ == "__main__":
    setup_event_loop_policy()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.log_out("info", "User interrupted")
    except Exception as exc:
        logger.log_out("error", f"Execution failed: {exc}")
