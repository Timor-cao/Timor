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
from typing import Any, Dict, List, Tuple

import websockets

from MT.Flopotech.BaseMethod.log_module import logger
from MT.Flopotech.Config.More_Account import WEBSOCKET_PRIVATE_CONFIG
from MT.Flopotech.BaseMethod.operate_config import OperateConfig

warnings.filterwarnings("ignore", category=DeprecationWarning)


def setup_event_loop_policy() -> None:
    """Call before asyncio.run (Windows compatibility)."""
    if platform.system() == "Windows" and sys.version_info >= (3, 8):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
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


class MarketOrder:
    """Market order open."""

    def __init__(self) -> None:
        self.config = WEBSOCKET_PRIVATE_CONFIG

        self.batch_size = _to_int(self.config.get("batch_size", 1000), 1000)
        self.sleep_time = _to_float(self.config.get("sleep_time", 0), 0.0)

        self.client_ids = list(self.config.get("client_ids", []))
        self.client_id_cycle = itertools.cycle(self.client_ids)
        self.accounts_headers: List[Tuple[str, Dict[str, str]]] = []

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

        max_concurrency = _to_int(self.config.get("max_concurrency", self.batch_size), self.batch_size)
        self._max_concurrency = max(1, max_concurrency)
        self._semaphore = asyncio.Semaphore(self._max_concurrency)

        self._max_retries = max(1, _to_int(self.config.get("max_retries", 2), 2))
        self._retry_delay = max(0.0, _to_float(self.config.get("retry_delay", 0.5), 0.5))
        self._open_timeout = max(0.1, _to_float(self.config.get("open_timeout", 3.0), 3.0))
        self._close_wait_timeout = max(1.0, _to_float(self.config.get("close_wait_timeout", 5.0), 5.0))

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
        finally:
            if websocket and not websocket.closed:
                try:
                    await websocket.close()
                    logger.log_out("info", f"Account {account_name} - WebSocket closed")
                except Exception as exc:
                    logger.log_out("warning", f"Account {account_name} - close error: {exc}")
        return message_count

    async def send_trading_request(self, account_name: str, headers: Dict[str, str]) -> None:
        """Send trading request and optionally receive responses."""
        url = self.config.get("websocket_url")
        if not url:
            logger.log_out("error", "websocket_url is missing")
            return

        receive_timeout = max(0.0, _to_float(self.config.get("receive_timeout", 0), 0.0))
        for attempt in range(1, self._max_retries + 1):
            websocket = None
            try:
                async with self._semaphore:
                    websocket = await websockets.connect(
                        url,
                        extra_headers=headers,
                        close_timeout=10,
                        ping_interval=30,
                        ping_timeout=10,
                        open_timeout=self._open_timeout,
                    )
                    logger.log_out("info", f"Account {account_name} - WebSocket connected")

                    request_message = json.dumps(self.order_data, ensure_ascii=False)
                    await websocket.send(request_message)
                    logger.log_out("info", f"Account {account_name} - order sent: {self.order_data['eventData']}")

                    if receive_timeout <= 0:
                        await websocket.close()
                        logger.log_out("info", f"Account {account_name} - receive disabled, closed")
                        return

                    await self.receive_websocket_messages(websocket, account_name, receive_timeout)
                    return
            except websockets.exceptions.WebSocketException as exc:
                logger.log_out(
                    "error", f"Account {account_name} - WebSocket error (attempt {attempt}): {exc}"
                )
            except Exception as exc:
                logger.log_out(
                    "error", f"Account {account_name} - unexpected error (attempt {attempt}): {exc}"
                )

            if websocket and not websocket.closed:
                try:
                    await websocket.close()
                except Exception as exc:
                    logger.log_out("warning", f"Account {account_name} - close error: {exc}")

            if attempt < self._max_retries and self._retry_delay > 0:
                await asyncio.sleep(self._retry_delay * attempt)
            else:
                logger.log_out(
                    "error",
                    f"Account {account_name} - reached max retries ({self._max_retries})",
                )

    async def send_subscribe_request(self) -> None:
        """Send trading request for each account."""
        headers_list = await self.generate_header()
        if not headers_list:
            logger.log_out("error", "No headers available, skip trading requests")
            return

        tasks = []
        for account_name, headers in headers_list:
            tasks.append(asyncio.create_task(self.send_trading_request(account_name, headers)))
            if self.sleep_time > 0:
                await asyncio.sleep(self.sleep_time)

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.log_out("error", f"Task {i} failed: {result}")

        logger.log_out("info", "All account trading requests finished")

    async def close(self) -> None:
        """Cleanup resources."""
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
