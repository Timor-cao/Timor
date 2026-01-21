#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
 @ Date   : 2026/1/21
 @ Author : Administrator
 @ Description :
   WebSocket 市价单批量开仓脚本（修复BUG并优化资源管理/并发/重试）
"""

from __future__ import annotations

import ast
import asyncio
import itertools
import json
import platform
import sys
import time
import warnings
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple, Union

import websockets


# 兼容在仓库缺失依赖时仍可运行（尽量不影响你原有工程结构）
try:
    from MT.Flopotech.BaseMethod.log_module import logger  # type: ignore
except Exception:  # pragma: no cover
    class _FallbackLogger:
        def log_out(self, level: str, msg: str) -> None:
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}][{level.upper()}] {msg}", file=sys.stderr if level.lower() in {"error", "warning"} else sys.stdout)

    logger = _FallbackLogger()

try:
    from MT.Flopotech.Config.More_Account import WEBSOCKET_PRIVATE_CONFIG  # type: ignore
except Exception:  # pragma: no cover
    WEBSOCKET_PRIVATE_CONFIG = {
        "websocket_url": "wss://example.invalid/ws",
        "client_ids": ["demo-client-id"],
        "receive_timeout": 0.3,
    }

try:
    from MT.Flopotech.BaseMethod.operate_config import OperateConfig  # type: ignore
except Exception:  # pragma: no cover
    class OperateConfig:  # 最小兜底，避免 import 失败导致无法运行
        def get_ini_value(self, section: str, key: str) -> str:
            raise RuntimeError("缺少 MT.Flopotech 依赖：无法读取 tokens 配置")


def setup_event_loop_policy() -> None:
    """在 asyncio.run 之前调用（适配 Windows）。"""
    if platform.system() == "Windows" and sys.version_info >= (3, 8):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    warnings.filterwarnings("ignore", category=ResourceWarning)
    warnings.filterwarnings("ignore", category=RuntimeWarning)


def _normalize_tokens(tokens_obj: Any) -> List[Tuple[str, str]]:
    """
    兼容多种 tokens 结构：
    - [(account, token), ...]
    - {"account": "token", ...}
    - (("account","token"), ...)
    返回统一结构 List[(account, token)]
    """
    if tokens_obj is None:
        return []

    if isinstance(tokens_obj, Mapping):
        items = list(tokens_obj.items())
        return [(str(k), str(v)) for k, v in items]

    if isinstance(tokens_obj, (list, tuple)):
        out: List[Tuple[str, str]] = []
        for item in tokens_obj:
            if isinstance(item, (list, tuple)) and len(item) == 2:
                out.append((str(item[0]), str(item[1])))
            else:
                raise ValueError(f"tokens 列表元素格式不正确: {item!r}")
        return out

    raise ValueError(f"不支持的 tokens 类型: {type(tokens_obj)!r}")


@dataclass(frozen=True)
class AccountHeader:
    account_name: str
    headers: Dict[str, str]


class MarketOrder:
    """市价单开仓"""

    def __init__(
        self,
        *,
        batch_size: int = 10,
        sleep_time: float = 0.1,
        max_retries: int = 2,
        retry_backoff_base: float = 1.0,
    ) -> None:
        self.batch_size = max(1, int(batch_size))
        self.sleep_time = max(0.0, float(sleep_time))
        self.max_retries = max(1, int(max_retries))
        self.retry_backoff_base = max(0.0, float(retry_backoff_base))

        self.config: Dict[str, Any] = dict(WEBSOCKET_PRIVATE_CONFIG)
        client_ids = self.config.get("client_ids") or []
        if not isinstance(client_ids, (list, tuple)) or not client_ids:
            raise ValueError("配置 WEBSOCKET_PRIVATE_CONFIG['client_ids'] 不能为空且必须为 list/tuple")
        self.client_id_cycle = itertools.cycle([str(x) for x in client_ids])

        self.order_data: Dict[str, Any] = {
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

        # 跟踪后台接收任务（用 set 更适合 add/remove）
        self._background_tasks: "set[asyncio.Task[Any]]" = set()

    async def generate_headers(self) -> List[AccountHeader]:
        """生成每个账号的 headers。"""
        try:
            tokens_str = OperateConfig().get_ini_value("PARAMS", "tokens")
            tokens_obj = ast.literal_eval(tokens_str)
            pairs = _normalize_tokens(tokens_obj)
            if not pairs:
                logger.log_out("error", "tokens 为空，无法生成请求头")
                return []

            out: List[AccountHeader] = []
            for account, token in pairs:
                headers = {
                    "Access-Token": token,
                    "Client-ID": next(self.client_id_cycle),
                }
                out.append(AccountHeader(account_name=str(account), headers=headers))
            return out
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.log_out("error", f"生成请求头失败: {e!s}")
            return []

    async def _receive_websocket_messages(
        self,
        websocket: websockets.WebSocketClientProtocol,
        account_name: str,
        timeout_duration: float,
    ) -> int:
        """
        接收 WebSocket 消息（后台执行）。
        - 超过 timeout_duration 没收到消息则退出
        - 退出前确保关闭连接
        """
        message_count = 0
        try:
            while True:
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=timeout_duration)
                    try:
                        message_data = json.loads(message)
                    except json.JSONDecodeError:
                        logger.log_out("warning", f"账号 {account_name} - 消息不是合法 JSON: {message!r}")
                        continue
                    message_count += 1
                    logger.log_out("debug", f"账号 {account_name} - 收到消息: {message_data}")
                except asyncio.TimeoutError:
                    logger.log_out("info", f"账号 {account_name} - 接收结束，共接收 {message_count} 条消息")
                    break
        except asyncio.CancelledError:
            # close() 时可能取消任务，确保连接关闭
            raise
        except Exception as e:
            logger.log_out("warning", f"账号 {account_name} - 接收消息异常: {e!s}")
        finally:
            if websocket and not websocket.closed:
                try:
                    await websocket.close()
                    logger.log_out("info", f"账号 {account_name} - WebSocket 连接已关闭")
                except Exception as e:
                    logger.log_out("warning", f"账号 {account_name} - 关闭连接时出错: {e!s}")
        return message_count

    def _track_background_task(self, task: "asyncio.Task[Any]") -> None:
        self._background_tasks.add(task)

        def _done_callback(t: "asyncio.Task[Any]") -> None:
            self._background_tasks.discard(t)

        task.add_done_callback(_done_callback)

    async def send_trading_request(self, account_name: str, headers: Dict[str, str]) -> None:
        """连接 WebSocket -> 发送交易请求 -> 后台接收（本协程不等待接收完成）。"""
        url = self.config.get("websocket_url")
        if not url:
            logger.log_out("error", "配置 websocket_url 为空，无法连接")
            return

        timeout_duration = float(self.config.get("receive_timeout", 0.3))
        timeout_duration = max(0.05, timeout_duration)

        websocket: Optional[websockets.WebSocketClientProtocol] = None

        for attempt in range(1, self.max_retries + 1):
            try:
                websocket = await websockets.connect(
                    str(url),
                    extra_headers=headers,
                    close_timeout=10,
                    ping_interval=30,
                    ping_timeout=10,
                )
                logger.log_out("info", f"账号 {account_name} - WebSocket 连接成功")

                request_message = json.dumps(self.order_data, ensure_ascii=False)
                await websocket.send(request_message)
                logger.log_out("info", f"账号 {account_name} - 已发送交易请求: {self.order_data.get('eventData')}")

                # 创建后台接收任务，连接由接收任务负责关闭
                receive_task = asyncio.create_task(
                    self._receive_websocket_messages(websocket, account_name, timeout_duration)
                )
                self._track_background_task(receive_task)
                return

            except (websockets.exceptions.WebSocketException, OSError) as e:
                logger.log_out("error", f"账号 {account_name} - WebSocket 连接/发送失败(第 {attempt}/{self.max_retries} 次): {e!s}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.log_out("error", f"账号 {account_name} - 未预期异常(第 {attempt}/{self.max_retries} 次): {e!s}")
            finally:
                # 如果没成功创建接收任务，就在这里释放连接
                if websocket is not None and websocket.closed is False:
                    try:
                        await websocket.close()
                    except Exception:
                        pass
                websocket = None

            if attempt < self.max_retries:
                delay = self.retry_backoff_base * attempt
                if delay > 0:
                    await asyncio.sleep(delay)

        logger.log_out("error", f"账号 {account_name} - 已达到最大重试次数，放弃本次请求")

    async def send_subscribe_request(self) -> None:
        """为每个用户分别发送交易请求（分批并发）。"""
        headers_list = await self.generate_headers()
        if not headers_list:
            logger.log_out("error", "无法获取用户请求头，跳过交易请求")
            return

        total_accounts = len(headers_list)
        batch_size = self.batch_size

        for batch_start in range(0, total_accounts, batch_size):
            batch_end = min(batch_start + batch_size, total_accounts)
            current_batch = headers_list[batch_start:batch_end]
            batch_no = batch_start // batch_size + 1

            logger.log_out("info", f"开始处理第 {batch_no} 批账号，共 {len(current_batch)} 个账号")

            tasks: List[asyncio.Task[None]] = []
            for item in current_batch:
                logger.log_out("info", f"开始发送交易请求,账号: {item.account_name} ...")
                tasks.append(asyncio.create_task(self.send_trading_request(item.account_name, item.headers)))

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for idx, r in enumerate(results):
                    if isinstance(r, Exception):
                        logger.log_out("error", f"第 {batch_no} 批 - 任务 {idx} 发送任务异常: {r!s}")
                logger.log_out("info", f"第 {batch_no} 批账号的交易请求发送完成")

            if batch_end < total_accounts and self.sleep_time > 0:
                logger.log_out("info", f"等待 {self.sleep_time} 秒后处理下一批账号...")
                await asyncio.sleep(self.sleep_time)

        logger.log_out("info", "所有批次交易请求已发送完成")

    async def close(self, *, wait_timeout: float = 5.0) -> None:
        """清理资源：等待后台接收任务完成（带超时）。"""
        tasks = list(self._background_tasks)
        if not tasks:
            logger.log_out("info", "无后台接收任务需要等待")
            return

        logger.log_out("info", f"等待 {len(tasks)} 个后台接收任务完成（超时 {wait_timeout} 秒）...")
        try:
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=wait_timeout)
        except asyncio.TimeoutError:
            logger.log_out("warning", "等待后台任务超时，将取消剩余任务")
            for t in list(self._background_tasks):
                t.cancel()
            await asyncio.gather(*list(self._background_tasks), return_exceptions=True)
        finally:
            self._background_tasks.clear()
            # 留一点时间给底层连接完成 close handshake
            await asyncio.sleep(0.1)
            logger.log_out("info", "资源清理完成")


async def main() -> None:
    market_order = MarketOrder()
    try:
        logger.log_out("info", "开始发送 WebSocket 交易请求...")
        await market_order.send_subscribe_request()
    finally:
        await market_order.close()


if __name__ == "__main__":
    setup_event_loop_policy()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.log_out("info", "用户中断程序执行")
    except Exception as e:
        logger.log_out("error", f"程序执行失败: {e!s}")
