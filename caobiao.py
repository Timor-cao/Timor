#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
 @ Date   : 2026/4/2
 @ Author : Administrator
 @ Description : 多账户市价单开仓（优化版）
"""
import ssl
import ast
import asyncio
import itertools
import sys
import time
import warnings
import websockets
import json
import platform
from MT.Flopotech.BaseMethod.log_module import logger
from MT.Flopotech.Config.More_Account import WEBSOCKET_PRIVATE_CONFIG
from MT.Flopotech.BaseMethod.operate_config import OperateConfig

warnings.filterwarnings("ignore", category=DeprecationWarning)


def setup_event_loop_policy() -> None:
    """在 asyncio.run 之前调用（适配 Windows）。"""
    if platform.system() == "Windows" and sys.version_info >= (3, 8):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    warnings.filterwarnings("ignore", category=ResourceWarning)
    warnings.filterwarnings("ignore", category=RuntimeWarning)


class ConnectionPool:
    """WebSocket连接池，实现连接复用与并发安全。"""

    def __init__(self, config):
        self.config = config
        self.connections = {}
        self._semaphore = asyncio.Semaphore(100)
        self._lock = asyncio.Lock()
        self._account_locks = {}
        self._closed = False

    async def _get_account_lock(self, account_name: str) -> asyncio.Lock:
        async with self._lock:
            lock = self._account_locks.get(account_name)
            if lock is None:
                lock = asyncio.Lock()
                self._account_locks[account_name] = lock
            return lock

    async def remove_connection(self, account_name: str):
        """安全移除并关闭指定账户连接。"""
        conn = None
        async with self._lock:
            conn = self.connections.pop(account_name, None)
        if conn and not conn.closed:
            try:
                await conn.close()
            except Exception:
                pass

    async def get_connection(self, account_name, headers):
        """获取或创建WebSocket连接（同账户串行建连）。"""
        if self._closed:
            return None

        account_lock = await self._get_account_lock(account_name)
        async with account_lock:
            if self._closed:
                return None

            async with self._lock:
                conn = self.connections.get(account_name)
                if conn and not conn.closed:
                    return conn
                if conn and conn.closed:
                    self.connections.pop(account_name, None)

            async with self._semaphore:
                try:
                    url = self.config["websocket_url"]
                    ssl_context = ssl.create_default_context()
                    ssl_context.check_hostname = False
                    ssl_context.verify_mode = ssl.CERT_NONE
                    new_conn = await websockets.connect(
                        url,
                        extra_headers=headers,
                        close_timeout=2,
                        ping_interval=20,
                        ping_timeout=10,
                        open_timeout=2.0,
                        ssl=ssl_context,
                        max_size=2 ** 20,
                        compression=None,
                    )
                except Exception:
                    return None

            old_conn = None
            async with self._lock:
                if self._closed:
                    old_conn = new_conn
                else:
                    old_conn = self.connections.get(account_name)
                    self.connections[account_name] = new_conn

            if old_conn and old_conn is not new_conn and not old_conn.closed:
                try:
                    await old_conn.close()
                except Exception:
                    pass

            if self._closed and new_conn and not new_conn.closed:
                try:
                    await new_conn.close()
                except Exception:
                    pass
                return None

            return new_conn

    async def close_all(self):
        """关闭所有连接。"""
        if self._closed:
            return

        self._closed = True
        async with self._lock:
            connections_to_close = list(self.connections.values())
            self.connections.clear()

        close_tasks = []
        for conn in connections_to_close:
            try:
                if not conn.closed:
                    close_tasks.append(conn.close())
            except Exception:
                pass

        if close_tasks:
            try:
                await asyncio.wait_for(asyncio.gather(*close_tasks, return_exceptions=True), timeout=5.0)
            except asyncio.TimeoutError:
                pass


class MarketOrder:
    """市价单开仓。"""

    def __init__(self):
        self.orders_per_second = 100
        self.interval = 1.0 / self.orders_per_second
        self.config = WEBSOCKET_PRIVATE_CONFIG
        self.client_id_cycle = itertools.cycle(WEBSOCKET_PRIVATE_CONFIG["client_ids"])
        self.tokens = {}
        self._semaphore = asyncio.Semaphore(100)
        self.connection_pool = ConnectionPool(self.config)

        self.background_tasks = set()
        self.reader_tasks = {}
        self._send_locks = {}
        self._send_locks_guard = asyncio.Lock()
        self._stop_event = asyncio.Event()

        self._max_retries = 1
        self._retry_delay = 0.1
        self._recv_timeout = 0.5
        self._max_inflight_tasks = 500

        self._order_template = {
            "eventType": "marketOrder",
            "eventData": {
                "action": "open",
                "orderFrom": "SELF",
                "symbolCode": "EURUSD",
                "side": "1",
                "leverage": "100",
                "openOrderAmt": "100",
                "language": "zh",
            },
        }

    async def _get_send_lock(self, account_name: str) -> asyncio.Lock:
        async with self._send_locks_guard:
            lock = self._send_locks.get(account_name)
            if lock is None:
                lock = asyncio.Lock()
                self._send_locks[account_name] = lock
            return lock

    async def generate_header(self):
        """生成每个账号的 headers。"""
        try:
            tokens_str = OperateConfig().get_ini_value("PARAMS", "tokens")
            self.tokens = ast.literal_eval(tokens_str)
            headers_list = []
            for account, token, actid in self.tokens:
                _ = actid
                headers = {
                    "Access-Token": token,
                    "Client-ID": next(self.client_id_cycle),
                }
                headers_list.append((str(account), headers))
            return headers_list
        except KeyboardInterrupt:
            logger.log_out("info", "用户中断程序执行")
            return []
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.log_out("error", f"生成请求头失败: {str(e)}")
            return []

    async def _start_reader_if_needed(self, websocket, account_name):
        """每个账户连接只启动一个 reader，避免并发 recv。"""
        old_task = self.reader_tasks.get(account_name)
        if old_task and not old_task.done():
            return

        reader_task = asyncio.create_task(
            self.receive_websocket_messages(websocket, account_name, self._recv_timeout)
        )
        self.reader_tasks[account_name] = reader_task
        self.background_tasks.add(reader_task)

        def _done(task):
            self.background_tasks.discard(task)
            current = self.reader_tasks.get(account_name)
            if current is task:
                self.reader_tasks.pop(account_name, None)
            try:
                _ = task.exception()
            except Exception:
                pass

        reader_task.add_done_callback(_done)

    async def receive_websocket_messages(self, websocket, account_name, timeout_duration):
        """接收 WebSocket 消息（后台执行）。"""
        message_count = 0
        try:
            recv = websocket.recv
            loads = json.loads
            while not self._stop_event.is_set():
                try:
                    message = await asyncio.wait_for(recv(), timeout=timeout_duration)
                    try:
                        loads(message)
                    except json.JSONDecodeError:
                        continue
                    message_count += 1
                except asyncio.TimeoutError:
                    continue
                except websockets.exceptions.ConnectionClosed:
                    break
                except asyncio.CancelledError:
                    break
                except Exception:
                    break
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
        return message_count

    async def send_trading_request(self, account_name, headers):
        """发送交易请求。"""
        max_retries = max(1, int(self._max_retries))
        retry_delay = max(0.0, float(self._retry_delay))
        send_lock = await self._get_send_lock(account_name)

        async with self._semaphore:
            for attempt in range(max_retries):
                try:
                    async with send_lock:
                        websocket = await self.connection_pool.get_connection(account_name, headers)
                        if not websocket:
                            if attempt < max_retries - 1 and retry_delay > 0:
                                await asyncio.sleep(retry_delay * (attempt + 1))
                            continue

                        await self._start_reader_if_needed(websocket, account_name)

                        request_data = {
                            "eventType": self._order_template["eventType"],
                            "eventData": {
                                **self._order_template["eventData"],
                                "reqTime": int(time.time() * 1000000),
                            },
                        }
                        request_message = json.dumps(request_data, ensure_ascii=False)
                        await websocket.send(request_message)
                        break
                except websockets.exceptions.ConnectionClosed:
                    await self.connection_pool.remove_connection(account_name)
                    if attempt < max_retries - 1 and retry_delay > 0:
                        await asyncio.sleep(retry_delay * (attempt + 1))
                except asyncio.CancelledError:
                    raise
                except Exception:
                    if attempt < max_retries - 1 and retry_delay > 0:
                        await asyncio.sleep(retry_delay * (attempt + 1))

    async def send_subscribe_request(self):
        """持续发送交易请求，控制每秒发送100个，使用header轮询。"""
        headers_list = await self.generate_header()
        if not headers_list:
            logger.log_out("error", "无法获取用户请求头，跳过交易请求")
            return

        logger.log_out("info", f"开始持续发送消息，每秒100条，使用 {len(headers_list)} 个账号轮询")

        start_time = time.monotonic()
        order_count = 0
        batch_size = 10
        header_index = 0
        header_count = len(headers_list)

        try:
            while not self._stop_event.is_set():
                if order_count > 0 and order_count % batch_size == 0:
                    expected_time = start_time + order_count * self.interval
                    current_time = time.monotonic()
                    delay = expected_time - current_time
                    if delay > 0:
                        await asyncio.sleep(delay)
                    else:
                        # 即使“落后于计划”，也主动让出事件循环，避免饥饿。
                        await asyncio.sleep(0)
                else:
                    await asyncio.sleep(0)

                account_name, headers = headers_list[header_index % header_count]
                header_index += 1

                task = asyncio.create_task(self.send_trading_request(account_name, headers))
                self.background_tasks.add(task)
                order_count += 1

                def _done(t):
                    self.background_tasks.discard(t)
                    try:
                        _ = t.exception()
                    except Exception:
                        pass

                task.add_done_callback(_done)

                # 控制在途任务上限，防止任务无限增长。
                if len(self.background_tasks) >= self._max_inflight_tasks:
                    done, _ = await asyncio.wait(
                        self.background_tasks,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for finished in done:
                        self.background_tasks.discard(finished)

                if order_count % 1000 == 0:
                    current_time = time.monotonic()
                    elapsed = current_time - start_time
                    actual_rate = order_count / elapsed if elapsed > 0 else 0
                    logger.log_out(
                        "info",
                        f"已发送 {order_count} 条消息，耗时 {elapsed:.2f} 秒，实际速率: {actual_rate:.2f} 个/秒",
                    )
        except KeyboardInterrupt:
            logger.log_out("info", "用户中断发送")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.log_out("error", f"发送过程中发生错误: {str(e)}")
        finally:
            end_time = time.monotonic()
            elapsed = end_time - start_time
            actual_rate = order_count / elapsed if elapsed > 0 else 0
            logger.log_out(
                "info",
                f"发送结束，共发送 {order_count} 个订单，耗时 {elapsed:.2f} 秒，实际速率: {actual_rate:.2f} 个/秒",
            )

    async def close(self):
        """清理资源。"""
        self._stop_event.set()

        # 1) 取消并等待后台任务（包含发送和reader任务）
        if self.background_tasks:
            tasks_to_cancel = list(self.background_tasks)
            for task in tasks_to_cancel:
                if not task.done():
                    try:
                        task.cancel()
                    except Exception:
                        pass
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks_to_cancel, return_exceptions=True),
                    timeout=3.0,
                )
            except asyncio.TimeoutError:
                pass
            self.background_tasks.clear()

        # 2) 关闭连接池
        try:
            await self.connection_pool.close_all()
        except Exception:
            pass

        await asyncio.sleep(0.01)
        logger.log_out("info", "资源清理已完成")


async def main():
    main_start_time = time.time()
    market_order = MarketOrder()

    try:
        logger.log_out("info", "开始发送 WebSocket 请求...")
        await market_order.send_subscribe_request()
    except KeyboardInterrupt:
        logger.log_out("info", "用户中断程序执行")
    except Exception as e:
        logger.log_out("error", f"WebSocket请求执行失败: {str(e)}")
    finally:
        try:
            await asyncio.wait_for(market_order.close(), timeout=10.0)
        except asyncio.TimeoutError:
            logger.log_out("warning", "资源清理超时，强制退出")
        except Exception as close_error:
            logger.log_out("error", f"资源关闭异常: {str(close_error)}")

    main_end_time = time.time()
    logger.log_out("info", f"执行完成，总耗时: {main_end_time - main_start_time:.4f} 秒")


if __name__ == "__main__":
    setup_event_loop_policy()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.log_out("info", "用户中断程序执行")
    except Exception as errorh:
        logger.log_out("error", f"程序执行失败: {str(errorh)}")
