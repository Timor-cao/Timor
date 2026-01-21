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
import warnings
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import websockets

from MT.Flopotech.BaseMethod.log_module import logger
from MT.Flopotech.BaseMethod.operate_config import OperateConfig
from MT.Flopotech.Config.More_Account import WEBSOCKET_PRIVATE_CONFIG


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
        return [(str(k), str(v)) for k, v in tokens_obj.items()]
    if isinstance(tokens_obj, (list, tuple)):
        out: List[Tuple[str, str]] = []
        for item in tokens_obj:
            if isinstance(item, (list, tuple)) and len(item) == 2:
                out.append((str(item[0]), str(item[1])))
            else:
                raise ValueError(f"tokens 列表元素格式不正确: {item!r}")
        return out
    raise ValueError(f"不支持的 tokens 类型: {type(tokens_obj)!r}")


class MarketOrder:
    """市价单开仓"""

    def __init__(self):
        # 性能优化：默认并发数加大，并避免每批 sleep（可通过配置覆盖）
        self.config = WEBSOCKET_PRIVATE_CONFIG
        self.batch_size = int(self.config.get("batch_size", 100))  # 实际并发上限（也用于兼容旧逻辑的“批大小”概念）
        self.sleep_time = float(self.config.get("sleep_time", 0.0))  # 批与批之间 sleep（提速默认 0）

        self.client_id_cycle = itertools.cycle(self.config["client_ids"])
        self.tokens = {}
        self.accounts_headers = {}

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

        # 性能优化：用 set 跟踪后台任务，增删为 O(1)
        self.background_tasks: "set[asyncio.Task]" = set()
        # 性能优化：用信号量控制并发，避免“分批+sleep”带来的节奏开销
        self._semaphore = asyncio.Semaphore(max(1, int(self.config.get("max_concurrency", self.batch_size))))

        # 重试参数（可配置）
        self._max_retries = int(self.config.get("max_retries", 2))
        self._retry_delay = float(self.config.get("retry_delay", 0.5))  # 提速：默认比原来更短

    async def generate_header(self):
        """生成每个账号的 headers"""
        try:
            tokens_str = OperateConfig().get_ini_value("PARAMS", "tokens")
            tokens_obj = ast.literal_eval(tokens_str)
            pairs = _normalize_tokens(tokens_obj)

            accounts_headers: List[Tuple[str, Dict[str, str]]] = []
            next_client_id = self.client_id_cycle.__next__
            for account, token in pairs:
                headers = {
                    "Access-Token": token,
                    "Client-ID": next_client_id(),
                }
                accounts_headers.append((str(account), headers))

            self.accounts_headers = accounts_headers
            return self.accounts_headers
        except KeyboardInterrupt:
            logger.log_out("info", "用户中断程序执行")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.log_out("error", f"生成请求头失败: {str(e)}")
            return []

    async def receive_websocket_messages(self, websocket, account_name, timeout_duration):
        """接收WebSocket消息（后台执行）"""
        message_count = 0
        try:
            # 小优化：本地化引用，减少属性查找
            recv = websocket.recv
            log_out = logger.log_out
            loads = json.loads

            while True:
                try:
                    message = await asyncio.wait_for(recv(), timeout=timeout_duration)
                    try:
                        message_data = loads(message)
                    except json.JSONDecodeError:
                        # 不让异常路径拖慢主流程
                        log_out("warning", f"账号 {account_name} - 消息解析失败(非JSON): {message!r}")
                        continue
                    message_count += 1
                    log_out("debug", f"账号 {account_name} - 收到消息: {message_data}")

                except asyncio.TimeoutError:
                    log_out("info", f"账号 {account_name} - 接收消息完成，共接收 {message_count} 条消息")
                    break
                except json.JSONDecodeError as errord:
                    log_out("error", f"账号 {account_name} - 消息解析失败: {str(errord)}")
                    continue
        finally:
            # 确保WebSocket连接被关闭
            if websocket and not websocket.closed:
                try:
                    await websocket.close()
                    logger.log_out("info", f"账号 {account_name} - WebSocket 连接已关闭")
                except Exception as errori:
                    logger.log_out("warning", f"账号 {account_name} - 关闭 WebSocket 连接时出错: {str(errori)}")
        return message_count

    async def send_trading_request(self, account_name, headers):
        """发送交易请求（发送后立即返回）"""
        websocket = None

        url = self.config["websocket_url"]
        timeout_duration = float(self.config.get("receive_timeout", 0.1))
        max_retries = max(1, int(self._max_retries))
        retry_delay = max(0.0, float(self._retry_delay))

        # 性能优化：控制并发，避免分批+sleep 的吞吐损失
        async with self._semaphore:
            for attempt in range(max_retries):
                try:
                    websocket = await websockets.connect(
                        url,
                        extra_headers=headers,
                        close_timeout=10,
                        ping_interval=30,
                        ping_timeout=10,
                        open_timeout=float(self.config.get("open_timeout", 3)),  # 提速：连接失败更快返回
                    )
                    logger.log_out("info", f"账号 {account_name} - WebSocket 连接成功")

                    request_message = json.dumps(self.order_data, ensure_ascii=False)
                    await websocket.send(request_message)
                    logger.log_out("info", f"账号 {account_name} - 已发送交易请求: {self.order_data['eventData']}")

                    # 发送成功后，创建后台任务接收消息，不再等待
                    receive_task = asyncio.create_task(
                        self.receive_websocket_messages(websocket, account_name, timeout_duration)
                    )
                    self.background_tasks.add(receive_task)

                    def task_done_callback(task):
                        self.background_tasks.discard(task)

                    receive_task.add_done_callback(task_done_callback)
                    break

                except websockets.exceptions.WebSocketException as errorf:
                    logger.log_out("error", f"账号 {account_name} - WebSocket 连接失败 (尝试 {attempt + 1}): {str(errorf)}")
                    if attempt < max_retries - 1 and retry_delay > 0:
                        await asyncio.sleep(retry_delay * (attempt + 1))
                    else:
                        logger.log_out("error", f"账号 {account_name} - WebSocket 连接最终失败，已达到最大重试次数")
                        if websocket and not websocket.closed:
                            await websocket.close()

                except Exception as errorg:
                    logger.log_out("error", f"账号 {account_name} - 发生未预期的错误 (尝试 {attempt + 1}): {str(errorg)}")
                    if attempt < max_retries - 1 and retry_delay > 0:
                        await asyncio.sleep(retry_delay * (attempt + 1))
                    else:
                        logger.log_out("error", f"账号 {account_name} - 操作最终失败，已达到最大重试次数")
                        if websocket and not websocket.closed:
                            await websocket.close()

    async def send_subscribe_request(self):
        """为每个用户的header分别发送交易请求"""
        HEADERS = await self.generate_header()
        if not HEADERS:
            logger.log_out("error", "无法获取用户请求头，跳过交易请求")
            return

        # 性能优化：不再严格“分批+等待”，改为一次性调度全部任务，由 semaphore 控制并发上限
        tasks = []
        for account_name, headers in HEADERS:
            logger.log_out("info", f"开始发送交易请求,账号:{account_name} ...")
            tasks.append(asyncio.create_task(self.send_trading_request(account_name, headers)))

            # 兼容保留：如果你仍希望节流，可在配置里设置 sleep_time > 0
            if self.sleep_time > 0:
                await asyncio.sleep(self.sleep_time)

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.log_out("error", f"任务 {i} 发送失败: {str(result)}")

        logger.log_out("info", "All accounts transaction requests sent completed")

    async def close(self):
        """Ways to Clean Up Resources"""
        if self.background_tasks:
            logger.log_out("info", f"Wait for {len(self.background_tasks)} background receive tasks to complete...")
            try:
                await asyncio.wait_for(
                    asyncio.gather(*list(self.background_tasks), return_exceptions=True),
                    timeout=float(self.config.get("close_wait_timeout", 5.0)),
                )
            except asyncio.TimeoutError:
                logger.log_out("warning", "等待后台接收任务超时，取消剩余任务")
                for t in list(self.background_tasks):
                    t.cancel()
                await asyncio.gather(*list(self.background_tasks), return_exceptions=True)

        await asyncio.sleep(0.1)
        logger.log_out("info", "Resource Cleanup Completed")


async def main():
    market_order = MarketOrder()
    try:
        logger.log_out("info", "Start WebSocket sending request...")
        await market_order.send_subscribe_request()
    finally:
        await market_order.close()


if __name__ == "__main__":
    setup_event_loop_policy()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.log_out("info", "User Interrupts Program Execution")
    except Exception as errorh:
        logger.log_out("error", f"Program execution failed: {str(errorh)}")
