#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
@ Date   : 2026/4/20
@ Author : Administrator
@ Description : 使用持久化 WebSocket 连接批量发送市价开仓请求。
"""

import ast
import asyncio
import copy
import inspect
import itertools
import json
import platform
import ssl
import sys
import time
import warnings
from typing import Any, Dict, List, Optional, Tuple

import websockets

from MT.Flopotech.BaseMethod.log_module import logger
from MT.Flopotech.BaseMethod.operate_config import OperateConfig
from MT.Flopotech.Config.More_Account import WEBSOCKET_PRIVATE_CONFIG

warnings.filterwarnings("ignore", category=DeprecationWarning)


HeaderItem = Tuple[str, Dict[str, str]]


def setup_event_loop_policy() -> None:
    """在 asyncio.run 之前调用，适配 Windows 事件循环策略。"""
    if platform.system() == "Windows" and sys.version_info >= (3, 8):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    warnings.filterwarnings("ignore", category=ResourceWarning)
    warnings.filterwarnings("ignore", category=RuntimeWarning)


class MarketOrder:
    """按固定频率向多个账号发送市价开仓 WebSocket 请求。"""

    def __init__(self, total_requests: int = 10):
        self.total_requests = total_requests
        self.config = WEBSOCKET_PRIVATE_CONFIG
        self.url = self.config["websocket_url"]
        self.client_id_cycle = itertools.cycle(WEBSOCKET_PRIVATE_CONFIG["client_ids"])
        self.tokens = ast.literal_eval(
            OperateConfig().get_ini_value(
                "Concurrent", "concurrentusers", "Concurrent_Users.ini"
            )
        )
        self.order_data = {
            "eventType": "marketOrder",
            "eventData": {
                "action": "open",
                "orderFrom": "SELF",
                "symbolCode": "XAUUSD",
                "side": "1",
                "leverage": "10",
                "openOrderAmt": "108",
                "language": "zh",
                "reqTime": int(time.time() * 1000000),
            },
        }

        self.background_tasks = set()
        self._close_wait_timeout = 30
        self.send_lock = asyncio.Lock()
        self.cached_headers: Optional[Dict[str, Dict[str, str]]] = None
        self.websocket_pool: Dict[str, Any] = {}
        self.connection_locks: Dict[str, asyncio.Lock] = {}
        self.connections_active = True
        self.send_total_time = 0.0
        self.close_total_time = 0.0

    async def generate_header(self) -> List[HeaderItem]:
        """生成每个账号的 headers，只生成一次，之后复用。"""
        if self.cached_headers is not None:
            return list(self.cached_headers.items())

        try:
            accounts_headers = {}
            for account, token, _actid in self.tokens:
                accounts_headers[str(account)] = {
                    "Access-Token": token,
                    "Client-ID": next(self.client_id_cycle),
                }
            self.cached_headers = accounts_headers
            logger.log_out("info", f"请求头已生成，共 {len(self.cached_headers)} 个账号")
            return list(accounts_headers.items())
        except KeyboardInterrupt:
            logger.log_out("info", "用户中断程序执行")
            return []
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.log_out("error", f"生成请求头失败: {exc}")
            return []

    @staticmethod
    def _is_websocket_closed(websocket: Any) -> bool:
        """兼容 websockets 新旧版本的连接关闭状态判断。"""
        if websocket is None:
            return True

        closed = getattr(websocket, "closed", None)
        if closed is not None:
            return bool(closed)

        state = getattr(websocket, "state", None)
        if state is None:
            return False
        return str(getattr(state, "name", state)).upper() in {"CLOSED", "CLOSING"}

    async def _connect_websocket(self, headers: Dict[str, str]) -> Any:
        """创建 WebSocket 连接，兼容 websockets 不同版本的请求头参数名。"""
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        connect_kwargs = {
            "close_timeout": 10,
            "ping_interval": 25,
            "ping_timeout": 20,
            "open_timeout": 20,
            "ssl": ssl_context,
        }
        try:
            signature = inspect.signature(websockets.connect)
            header_arg_name = (
                "additional_headers"
                if "additional_headers" in signature.parameters
                else "extra_headers"
            )
        except (TypeError, ValueError):
            header_arg_name = "additional_headers"
        connect_kwargs[header_arg_name] = headers
        return await websockets.connect(self.url, **connect_kwargs)

    async def _get_websocket(self, account_name: str, headers: Dict[str, str]) -> Any:
        """获取或创建 WebSocket 连接，确保每个账号同一时间只建一条连接。"""
        lock = self.connection_locks.setdefault(account_name, asyncio.Lock())
        async with lock:
            websocket = self.websocket_pool.get(account_name)
            if not self._is_websocket_closed(websocket):
                return websocket

            try:
                websocket = await self._connect_websocket(headers)
                self.websocket_pool[account_name] = websocket
                logger.log_out("info", f"账号 {account_name} - WebSocket 连接已建立")

                task = asyncio.create_task(
                    self._heartbeat_monitor(account_name, websocket)
                )
                self.background_tasks.add(task)
                task.add_done_callback(self.background_tasks.discard)
                return websocket
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.log_out("error", f"账号 {account_name} - 建立WebSocket连接失败: {exc}")
                return None

    async def _heartbeat_monitor(self, account_name: str, websocket: Any) -> None:
        """监控单条连接；断开后触发一次重连并结束当前监控任务。"""
        while self.connections_active:
            try:
                await asyncio.wait_for(websocket.wait_closed(), timeout=35)
                if not self.connections_active:
                    break

                logger.log_out("warning", f"账号 {account_name} - WebSocket 连接已断开，正在重连...")
                if self.websocket_pool.get(account_name) is websocket:
                    self.websocket_pool.pop(account_name, None)

                headers = self.cached_headers.get(account_name) if self.cached_headers else None
                if headers:
                    new_websocket = await self._get_websocket(account_name, headers)
                    if new_websocket:
                        logger.log_out("info", f"账号 {account_name} - WebSocket 重连成功")
                break
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.log_out("error", f"账号 {account_name} - 心跳监控异常: {exc}")
                await asyncio.sleep(0.1)

    def _build_request_message(self, req_time: int) -> str:
        """为当前发送批次构造独立消息，避免并发任务读取共享字典产生竞态。"""
        order_data = copy.deepcopy(self.order_data)
        order_data["eventData"]["reqTime"] = req_time
        return json.dumps(order_data, ensure_ascii=False)

    async def send_trading_request(
        self, account_name: str, headers: Dict[str, str], request_message: str
    ) -> None:
        """使用持久化 WebSocket 连接发送单个账号交易请求。"""
        try:
            websocket = await self._get_websocket(account_name, headers)
            if websocket is None:
                logger.log_out("error", f"账号 {account_name} - 无可用WebSocket连接")
                return

            if self._is_websocket_closed(websocket):
                logger.log_out("warning", f"账号 {account_name} - WebSocket连接已关闭，重新获取连接")
                self.websocket_pool.pop(account_name, None)
                websocket = await self._get_websocket(account_name, headers)
                if websocket is None:
                    logger.log_out("error", f"账号 {account_name} - 无法重新获取WebSocket连接")
                    return

            await websocket.send(request_message)
            logger.log_out("info", f"账号 {account_name} - 已发送交易请求")
        except websockets.exceptions.ConnectionClosed as exc:
            logger.log_out("error", f"账号 {account_name} - WebSocket连接已关闭: {exc}")
            self.websocket_pool.pop(account_name, None)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.log_out("error", f"账号 {account_name} - 发送失败: {exc}")
            self.websocket_pool.pop(account_name, None)

    async def receive_websocket_messages(
        self, websocket: Any, account_name: str, timeout_duration: float
    ) -> int:
        """在指定超时时间内接收 WebSocket 消息。"""
        message_count = 0
        try:
            while True:
                try:
                    message = await asyncio.wait_for(
                        websocket.recv(), timeout=timeout_duration
                    )
                except asyncio.TimeoutError:
                    break

                try:
                    message_data = json.loads(message)
                except json.JSONDecodeError:
                    logger.log_out("warning", f"账号 {account_name} - 消息解析失败(非JSON): {message!r}")
                    continue

                message_count += 1
                logger.log_out("debug", f"账号 {account_name} - 收到消息: {message_data}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.log_out("error", f"账号 {account_name} - 接收消息时发生错误: {exc}")
        return message_count

    async def send_concurrent_request(self) -> None:
        """每秒向所有账号发送一次请求，复用账号级持久化 WebSocket 连接。"""
        headers_items = await self.generate_header()
        if not headers_items:
            logger.log_out("error", "无法获取用户请求头，终止发送")
            return

        send_start_time = time.time()
        logger.log_out("info", f"开始发送请求，目标每秒发送一次，共发送 {self.total_requests} 次")

        for request_count in range(1, self.total_requests + 1):
            target_time = send_start_time + (request_count - 1)
            current_time = time.time()
            if current_time < target_time:
                wait_time = target_time - current_time
                logger.log_out("debug", f"等待 {wait_time:.3f} 秒后发送第 {request_count} 次请求")
                await asyncio.sleep(wait_time)

            async with self.send_lock:
                logger.log_out("info", f"第 {request_count} 次发送请求...")
                request_message = self._build_request_message(int(time.time() * 1000000))
                tasks = [
                    asyncio.create_task(
                        self.send_trading_request(account_name, headers, request_message)
                    )
                    for account_name, headers in headers_items
                ]
                self.background_tasks.update(tasks)
                for task in tasks:
                    task.add_done_callback(self.background_tasks.discard)
                await asyncio.gather(*tasks, return_exceptions=True)

        send_end_time = time.time()
        self.send_total_time = send_end_time - send_start_time
        logger.log_out(
            "info",
            f"所有请求发送完成，发送总时间: {self.send_total_time:.4f} 秒，准备关闭WebSocket连接",
        )

    async def _close_websocket(self, account_name: str, websocket: Any) -> None:
        """安全关闭一条 WebSocket 连接。"""
        if self._is_websocket_closed(websocket):
            return
        try:
            await websocket.close()
            logger.log_out("info", f"账号 {account_name} - WebSocket 连接已关闭")
        except Exception as exc:
            logger.log_out("warning", f"账号 {account_name} - 关闭WebSocket连接时出错: {exc}")

    async def close(self) -> None:
        """清理资源，关闭所有 WebSocket 连接和后台监控任务。"""
        close_start_time = time.time()
        self.connections_active = False

        close_tasks = [
            asyncio.create_task(self._close_websocket(account_name, websocket))
            for account_name, websocket in list(self.websocket_pool.items())
        ]
        if close_tasks:
            await asyncio.wait_for(
                asyncio.gather(*close_tasks, return_exceptions=True),
                timeout=self._close_wait_timeout,
            )

        if self.background_tasks:
            for task in list(self.background_tasks):
                if not task.done():
                    task.cancel()
            await asyncio.gather(*list(self.background_tasks), return_exceptions=True)

        self.websocket_pool.clear()
        self.connection_locks.clear()
        self.close_total_time = time.time() - close_start_time
        await asyncio.sleep(0.01)
        logger.log_out("info", f"资源清理已完成，关闭WebSocket总时间: {self.close_total_time:.4f} 秒")


async def main() -> None:
    main_start_time = time.time()
    total_requests = 10
    market_order = MarketOrder(total_requests)
    try:
        logger.log_out("info", f"开始发送市价开仓请求，共发送 {total_requests} 次，每秒1次...")
        await market_order.send_concurrent_request()
    except Exception as exc:
        logger.log_out("error", f"市价开仓请求执行失败: {exc}")
        raise
    finally:
        try:
            await market_order.close()
        except Exception as close_error:
            logger.log_out("error", f"资源关闭异常: {close_error}")

    total_time = time.time() - main_start_time

    logger.log_out("info", "=== 时间统计 ===")
    logger.log_out("info", f"执行总耗时: {total_time:.4f} 秒")
    logger.log_out("info", f"发送请求总时间: {market_order.send_total_time:.4f} 秒")
    logger.log_out("info", f"关闭WebSocket总时间: {market_order.close_total_time:.4f} 秒")
    logger.log_out("info", "=================")


if __name__ == "__main__":
    setup_event_loop_policy()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.log_out("info", "用户中断程序执行")
    except Exception as exc:
        logger.log_out("error", f"程序执行失败: {exc}")
    finally:
        logger.log_out("info", "程序执行结束.")
