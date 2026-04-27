#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
@ Date   : 2026/4/2
@ Author : Administrator
@ Description : 多账户市价单开仓，使用预建连接和绝对时间调度提高发送频率精度。
"""

import ast
import asyncio
import copy
import inspect
import itertools
import json
import os
import platform
import ssl
import statistics
import sys
import time
import warnings
from typing import Any, Dict, List, Optional, Sequence, Tuple

import websockets

from MT.Flopotech.BaseMethod.log_module import logger
from MT.Flopotech.BaseMethod.operate_config import OperateConfig
from MT.Flopotech.Config.More_Account import WEBSOCKET_PRIVATE_CONFIG

warnings.filterwarnings("ignore", category=DeprecationWarning)


HeaderItem = Tuple[str, Dict[str, str]]
ConnectionItem = Tuple[str, Any]


def _to_bool(value: Any, default: bool = False) -> bool:
    """兼容配置文件中的字符串布尔值。"""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def _get_env_or_config(config: Dict[str, Any], env_name: str, config_name: str, default: Any) -> Any:
    """优先读取环境变量，未设置时读取配置，最后使用默认值。"""
    return os.getenv(env_name, config.get(config_name, default))


def setup_event_loop_policy() -> None:
    """在 asyncio.run 之前调用，适配 Windows 事件循环策略。"""
    if platform.system() == "Windows" and sys.version_info >= (3, 8):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    warnings.filterwarnings("ignore", category=ResourceWarning)
    warnings.filterwarnings("ignore", category=RuntimeWarning)


class MarketOrder:
    """多账户市价单开仓压测客户端。"""

    def __init__(self) -> None:
        self.config = WEBSOCKET_PRIVATE_CONFIG
        self.url = self.config["websocket_url"]
        self.worker_index = int(
            _get_env_or_config(self.config, "WORKER_INDEX", "worker_index", 0)
        )
        self.worker_count = max(
            1,
            int(_get_env_or_config(self.config, "WORKER_COUNT", "worker_count", 1)),
        )
        if not 0 <= self.worker_index < self.worker_count:
            raise ValueError("WORKER_INDEX 必须在 [0, WORKER_COUNT) 范围内")

        configured_rate = float(self.config.get("orders_per_second", 200))
        self.global_rate = max(
            0.001,
            float(_get_env_or_config(self.config, "GLOBAL_RATE", "global_rate", configured_rate)),
        )
        self.orders_per_second = max(0.001, self.global_rate / self.worker_count)
        self.start_at = float(_get_env_or_config(self.config, "START_AT", "start_at", 0))
        self.run_id = str(_get_env_or_config(self.config, "RUN_ID", "run_id", "local"))
        self.result_file = str(
            _get_env_or_config(
                self.config,
                "RESULT_FILE",
                "result_file",
                f"market_order_results_{self.run_id}.jsonl",
            )
        )
        self.assigned_account_count = 0
        self.total_account_count = 0
        self.connection_concurrency = max(
            1, int(self.config.get("connection_concurrency", 100))
        )
        self.receive_timeout = float(self.config.get("receive_timeout", 3.0))
        self.open_timeout = float(self.config.get("open_timeout", 10.0))
        self.close_timeout = float(self.config.get("close_timeout", 10.0))
        self.ping_interval = float(self.config.get("ping_interval", 20.0))
        self.ping_timeout = float(self.config.get("ping_timeout", 10.0))
        self.max_retries = max(1, int(self.config.get("max_retries", 2)))
        self.retry_delay = max(0.0, float(self.config.get("retry_delay", 0.5)))
        self.spin_threshold = max(0.0, float(self.config.get("spin_threshold", 0.001)))
        self.busy_spin_threshold = max(
            0.0,
            min(
                self.spin_threshold,
                float(self.config.get("busy_spin_threshold", 0.0002)),
            ),
        )
        self.log_each_send = _to_bool(self.config.get("log_each_send"), False)

        self.client_id_cycle = itertools.cycle(self.config["client_ids"])
        self.tokens: Sequence[Tuple[Any, str, Any]] = []
        self.cached_headers: Optional[Dict[str, Dict[str, str]]] = None
        self.websocket_pool: Dict[str, Any] = {}
        self.receiver_tasks: set[asyncio.Task] = set()
        self.connection_semaphore = asyncio.Semaphore(self.connection_concurrency)
        self.send_delays_ms: List[float] = []
        self.send_errors = 0
        self.sent_count = 0
        self.connections_ready_count = 0
        self.send_total_time = 0.0
        self.close_total_time = 0.0

        self.order_data = {
            "eventType": "marketOrder",
            "eventData": {
                "action": "open",
                "orderFrom": "SELF",
                "symbolCode": self.config.get("symbol_code", "EURUSD"),
                "side": str(self.config.get("side", "1")),
                "leverage": str(self.config.get("leverage", "100")),
                "openOrderAmt": str(self.config.get("open_order_amt", "100")),
                "language": self.config.get("language", "zh"),
                "reqTime": int(time.time() * 1000000),
            },
        }
        logger.log_out(
            "info",
            f"Worker {self.worker_index}/{self.worker_count} 初始化完成，"
            f"run_id={self.run_id}, 全局目标速率={self.global_rate:.2f}/秒，"
            f"本 Worker 目标速率={self.orders_per_second:.2f}/秒",
        )

    def _load_tokens(self) -> Sequence[Tuple[Any, str, Any]]:
        """兼容常见配置位置读取账号 token。"""
        if self.tokens:
            return self.tokens

        operate_config = OperateConfig()
        tokens_str = ""
        try:
            tokens_str = operate_config.get_ini_value("PARAMS", "tokens")
        except Exception:
            tokens_str = ""

        if not tokens_str:
            tokens_str = operate_config.get_ini_value(
                "Concurrent", "concurrentusers", "Concurrent_Users.ini"
            )

        tokens = ast.literal_eval(tokens_str)
        if not isinstance(tokens, (list, tuple)):
            raise ValueError("tokens 配置必须是列表或元组")

        self.tokens = tokens
        return self.tokens

    async def generate_header(self) -> List[HeaderItem]:
        """为每个账号生成 WebSocket 请求头。"""
        if self.cached_headers is not None:
            return list(self.cached_headers.items())

        try:
            accounts_headers: Dict[str, Dict[str, str]] = {}
            for account, token, _actid in self._load_tokens():
                accounts_headers[str(account)] = {
                    "Access-Token": token,
                    "Client-ID": next(self.client_id_cycle),
                }

            all_headers = list(accounts_headers.items())
            sharded_headers = [
                item
                for index, item in enumerate(all_headers)
                if index % self.worker_count == self.worker_index
            ]
            self.total_account_count = len(all_headers)
            self.assigned_account_count = len(sharded_headers)
            self.cached_headers = dict(sharded_headers)
            logger.log_out(
                "info",
                f"请求头已生成，共 {self.total_account_count} 个账号；"
                f"Worker {self.worker_index}/{self.worker_count} 分配 "
                f"{self.assigned_account_count} 个账号",
            )
            return sharded_headers
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

    def _build_ssl_context(self) -> ssl.SSLContext:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        return ssl_context

    async def _connect_websocket(self, headers: Dict[str, str]) -> Any:
        """创建 WebSocket 连接，兼容 websockets 不同版本的请求头参数名。"""
        connect_kwargs = {
            "close_timeout": self.close_timeout,
            "ping_interval": self.ping_interval,
            "ping_timeout": self.ping_timeout,
            "open_timeout": self.open_timeout,
            "ssl": self._build_ssl_context(),
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

    async def _connect_account(self, account_name: str, headers: Dict[str, str]) -> Optional[ConnectionItem]:
        """带重试地为单个账号建立 WebSocket 连接。"""
        async with self.connection_semaphore:
            for attempt in range(1, self.max_retries + 1):
                try:
                    websocket = await self._connect_websocket(headers)
                    self.websocket_pool[account_name] = websocket
                    logger.log_out("info", f"账号 {account_name} - WebSocket 连接已建立")
                    return account_name, websocket
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.log_out(
                        "error",
                        f"账号 {account_name} - WebSocket 建连失败 "
                        f"(尝试 {attempt}/{self.max_retries}): {exc}",
                    )
                    if attempt < self.max_retries and self.retry_delay > 0:
                        await asyncio.sleep(self.retry_delay * attempt)
        return None

    async def prepare_connections(self, headers_items: Sequence[HeaderItem]) -> List[ConnectionItem]:
        """发送前预建所有可用连接，避免建连耗时干扰发送频率。"""
        tasks = [
            asyncio.create_task(self._connect_account(account_name, headers))
            for account_name, headers in headers_items
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        connections: List[ConnectionItem] = []
        for result in results:
            if isinstance(result, Exception):
                logger.log_out("error", f"预建连接任务异常: {result}")
            elif result is not None:
                connections.append(result)

        logger.log_out(
            "info",
            f"连接预热完成，可用连接 {len(connections)}/{len(headers_items)} 条",
        )
        self.connections_ready_count = len(connections)
        return connections

    def _build_request_message(self) -> str:
        """为每次发送构造独立订单消息，确保 reqTime 是当前发送时间。"""
        order_data = copy.deepcopy(self.order_data)
        order_data["eventData"]["reqTime"] = int(time.time() * 1000000)
        return json.dumps(order_data, ensure_ascii=False)

    async def _wait_until(self, target_time: float) -> None:
        """基于事件循环单调时钟等待到绝对目标时间。

        等待分三段：
        1. 距离目标时间较远时用 asyncio.sleep，降低 CPU 占用；
        2. 进入 spin_threshold 后用 sleep(0) 主动让出控制权，减少粗睡眠误差；
        3. 最后 busy_spin_threshold 内做短暂忙等，尽量贴近目标发送时刻。
        """
        loop = asyncio.get_running_loop()
        delay = target_time - loop.time()
        if delay <= 0:
            return

        if self.spin_threshold > 0 and delay > self.spin_threshold:
            await asyncio.sleep(delay - self.spin_threshold)

        if self.busy_spin_threshold > 0:
            while True:
                remaining = target_time - loop.time()
                if remaining <= 0:
                    return
                if remaining <= self.busy_spin_threshold:
                    break
                await asyncio.sleep(0)

            while loop.time() < target_time:
                pass
            return

        while loop.time() < target_time:
            await asyncio.sleep(0)

    async def receive_websocket_messages(self, websocket: Any, account_name: str) -> int:
        """后台接收服务端响应，直到超时或连接关闭。"""
        message_count = 0
        try:
            while not self._is_websocket_closed(websocket):
                try:
                    message = await asyncio.wait_for(
                        websocket.recv(), timeout=self.receive_timeout
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
        except websockets.exceptions.ConnectionClosed:
            pass
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.log_out("error", f"账号 {account_name} - 接收消息时发生错误: {exc}")
        return message_count

    def _start_receiver(self, account_name: str, websocket: Any) -> None:
        """发送成功后为当前连接启动后台接收任务。"""
        task = asyncio.create_task(
            self.receive_websocket_messages(websocket, account_name)
        )
        self.receiver_tasks.add(task)
        task.add_done_callback(self._receiver_done_callback(account_name))

    def _receiver_done_callback(self, account_name: str):
        def callback(task: asyncio.Task) -> None:
            self.receiver_tasks.discard(task)
            if task.cancelled():
                return
            try:
                task.result()
            except Exception as exc:
                logger.log_out("error", f"账号 {account_name} - 接收任务异常: {exc}")

        return callback

    async def send_with_precise_rate(self, connections: Sequence[ConnectionItem]) -> None:
        """按绝对时间调度发送，只把 websocket.send 纳入频率控制。"""
        if not connections:
            logger.log_out("error", "没有可用 WebSocket 连接，终止发送")
            return

        loop = asyncio.get_running_loop()
        interval = 1.0 / self.orders_per_second
        start_time = self._resolve_start_time(loop)
        first_send_time: Optional[float] = None
        last_send_time: Optional[float] = None

        logger.log_out(
            "info",
            f"开始精确调度发送，共 {len(connections)} 个订单，目标速率 "
            f"{self.orders_per_second:.2f} 个/秒",
        )

        for index, (account_name, websocket) in enumerate(connections):
            target_time = start_time + index * interval
            await self._wait_until(target_time)

            if self._is_websocket_closed(websocket):
                actual_time = loop.time()
                self.send_delays_ms.append((actual_time - target_time) * 1000)
                logger.log_out("error", f"账号 {account_name} - 发送前连接已关闭")
                self.send_errors += 1
                continue

            try:
                request_message = self._build_request_message()
                actual_time = loop.time()
                self.send_delays_ms.append((actual_time - target_time) * 1000)
                await websocket.send(request_message)
                if first_send_time is None:
                    first_send_time = actual_time
                last_send_time = loop.time()
                self.sent_count += 1
                self._start_receiver(account_name, websocket)
                if self.log_each_send:
                    logger.log_out("debug", f"账号 {account_name} - 已发送交易请求")
            except websockets.exceptions.ConnectionClosed as exc:
                logger.log_out("error", f"账号 {account_name} - WebSocket连接已关闭: {exc}")
                self.send_errors += 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.log_out("error", f"账号 {account_name} - 发送失败: {exc}")
                self.send_errors += 1

        if first_send_time is not None and last_send_time is not None:
            self.send_total_time = max(last_send_time - first_send_time, interval)
        self._log_send_statistics()

    def _resolve_start_time(self, loop: asyncio.AbstractEventLoop) -> float:
        """解析本轮发送开始时间；分布式模式建议所有 Worker 使用同一个 START_AT。"""
        if self.start_at > 0:
            wall_delay = self.start_at - time.time()
            if wall_delay > 0:
                logger.log_out(
                    "info",
                    f"Worker {self.worker_index}/{self.worker_count} 等待统一开始时间 "
                    f"START_AT={self.start_at:.6f}，剩余 {wall_delay:.3f} 秒",
                )
                return loop.time() + wall_delay

            logger.log_out(
                "warning",
                f"START_AT={self.start_at:.6f} 已早于当前时间，将立即开始发送",
            )
            return loop.time()

        return loop.time() + float(self.config.get("start_delay", 0.2))

    def _log_send_statistics(self) -> None:
        elapsed = self.send_total_time
        actual_rate = self.sent_count / elapsed if elapsed > 0 else 0

        logger.log_out(
            "info",
            f"发送完成，成功 {self.sent_count} 个，失败 {self.send_errors} 个，"
            f"耗时 {elapsed:.4f} 秒，实际速率 {actual_rate:.2f} 个/秒",
        )

        if not self.send_delays_ms:
            return

        sorted_delays = sorted(self.send_delays_ms)
        avg_delay = statistics.fmean(sorted_delays)
        max_delay = max(sorted_delays)
        p95_delay = self._percentile(sorted_delays, 95)
        p99_delay = self._percentile(sorted_delays, 99)

        logger.log_out(
            "info",
            "调度误差(ms): "
            f"avg={avg_delay:.3f}, max={max_delay:.3f}, "
            f"p95={p95_delay:.3f}, p99={p99_delay:.3f}",
        )

    def build_result(self, round_index: int, round_total_time: float) -> Dict[str, Any]:
        """构造当前 Worker 的压测结果，便于分布式汇总。"""
        sorted_delays = sorted(self.send_delays_ms)
        avg_delay = statistics.fmean(sorted_delays) if sorted_delays else 0.0
        max_delay = max(sorted_delays) if sorted_delays else 0.0

        return {
            "run_id": self.run_id,
            "round_index": round_index,
            "worker_index": self.worker_index,
            "worker_count": self.worker_count,
            "global_rate": self.global_rate,
            "worker_rate": self.orders_per_second,
            "accounts_total": self.total_account_count,
            "accounts_assigned": self.assigned_account_count,
            "connections_ready": self.connections_ready_count,
            "sent_count": self.sent_count,
            "send_errors": self.send_errors,
            "send_total_time": self.send_total_time,
            "round_total_time": round_total_time,
            "actual_rate": self.sent_count / self.send_total_time
            if self.send_total_time > 0
            else 0.0,
            "delay_avg_ms": avg_delay,
            "delay_max_ms": max_delay,
            "delay_p95_ms": self._percentile(sorted_delays, 95),
            "delay_p99_ms": self._percentile(sorted_delays, 99),
        }

    def write_result(self, round_index: int, round_total_time: float) -> None:
        """把当前 Worker 结果写入 JSONL 文件；多机结果可按 run_id 汇总。"""
        if not self.result_file:
            return

        result = self.build_result(round_index, round_total_time)
        try:
            with open(self.result_file, "a", encoding="utf-8") as file_obj:
                file_obj.write(json.dumps(result, ensure_ascii=False) + "\n")
            logger.log_out("info", f"Worker 结果已写入: {self.result_file}")
        except Exception as exc:
            logger.log_out("error", f"写入 Worker 结果失败: {exc}")

    @staticmethod
    def _percentile(sorted_values: Sequence[float], percentile: float) -> float:
        if not sorted_values:
            return 0.0
        index = round((len(sorted_values) - 1) * percentile / 100)
        return sorted_values[index]

    async def send_subscribe_request(self) -> None:
        """完整压测流程：生成请求头、预建连接、后台接收、精确发送。"""
        headers_items = await self.generate_header()
        if not headers_items:
            logger.log_out("error", "无法获取用户请求头，跳过交易请求")
            return

        connections = await self.prepare_connections(headers_items)
        await self.send_with_precise_rate(connections)

    async def _close_websocket(self, account_name: str, websocket: Any) -> None:
        if self._is_websocket_closed(websocket):
            return
        try:
            await websocket.close()
            logger.log_out("info", f"账号 {account_name} - WebSocket 连接已关闭")
        except Exception as exc:
            logger.log_out("warning", f"账号 {account_name} - 关闭 WebSocket 连接时出错: {exc}")

    async def close(self) -> None:
        """清理接收任务和 WebSocket 连接。"""
        close_start = time.perf_counter()

        close_tasks = [
            asyncio.create_task(self._close_websocket(account_name, websocket))
            for account_name, websocket in list(self.websocket_pool.items())
        ]
        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)

        if self.receiver_tasks:
            for task in list(self.receiver_tasks):
                if not task.done():
                    task.cancel()
            await asyncio.gather(*list(self.receiver_tasks), return_exceptions=True)

        self.websocket_pool.clear()
        self.receiver_tasks.clear()
        self.close_total_time = time.perf_counter() - close_start
        await asyncio.sleep(0.01)
        logger.log_out("info", f"资源清理已完成，关闭耗时: {self.close_total_time:.4f} 秒")


async def run_once(round_index: int) -> None:
    round_start = time.perf_counter()
    market_order = MarketOrder()
    try:
        logger.log_out("info", f"开始第 {round_index} 轮 WebSocket 市价开仓压测...")
        await market_order.send_subscribe_request()
    except Exception as exc:
        logger.log_out("error", f"第 {round_index} 轮 WebSocket 请求执行失败: {exc}")
        raise
    finally:
        try:
            await market_order.close()
        except Exception as close_error:
            logger.log_out("error", f"第 {round_index} 轮资源关闭异常: {close_error}")

    round_total_time = time.perf_counter() - round_start
    market_order.write_result(round_index, round_total_time)
    logger.log_out(
        "info",
        f"第 {round_index} 轮执行完成，总耗时: {round_total_time:.4f} 秒",
    )


async def main() -> None:
    rounds = int(WEBSOCKET_PRIVATE_CONFIG.get("rounds", 20))
    for round_index in range(1, rounds + 1):
        await run_once(round_index)


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
