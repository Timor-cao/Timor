#!/usr/bin/env python
# -*- coding:utf-8 -*-

import ast
import asyncio
import sys
import warnings
from typing import Dict, List, Tuple, Optional

import aiohttp
import websockets
import json
from MT.Flopotech.BaseMethod.log_module import logger
from MT.Flopotech.Config.More_Account import WEBSOCKET_PRIVATE_CONFIG, APP_URL, WEBSOCKET_PRIVATE_URL
import platform
from MT.Flopotech.BaseMethod.operate_config import OperateConfig


def setup_event_loop_policy() -> None:
    """须在 asyncio.run 之前调用（适配 Windows）。"""
    if platform.system() == "Windows" and sys.version_info >= (3, 8):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    warnings.filterwarnings("ignore", category=ResourceWarning)
    warnings.filterwarnings("ignore", category=RuntimeWarning)


class CloseOrderMore:
    """市价订单平仓"""

    def __init__(self, max_concurrent: int = 500):
        self.max_concurrent: int = max_concurrent  # 最大并发数
        self.semaphore: Optional[asyncio.Semaphore] = None  # 兼容保留：旧的全局信号量
        self.http_semaphore: Optional[asyncio.Semaphore] = None  # HTTP 并发信号量（不新增方法/类，仅新增属性）
        self.ws_semaphore: Optional[asyncio.Semaphore] = None  # WS 并发信号量（不新增方法/类，仅新增属性）

        self.config = WEBSOCKET_PRIVATE_CONFIG
        self.ws_url = WEBSOCKET_PRIVATE_URL
        self.hold_url = APP_URL + "/api/ord/order/hold"  # 获取持仓订单URL
        self.tokens: Dict[str, str] = {}
        self.accounts_headers: List[Tuple[str, Dict[str, str]]] = []
        self.session: Optional[aiohttp.ClientSession] = None  # 复用的aiohttp会话

    async def __aenter__(self):
        """支持异步上下文管理器"""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """支持异步上下文管理器"""
        await self.close()

    async def initialize(self):
        """初始化资源"""
        # 配置aiohttp会话，提高性能
        connector = aiohttp.TCPConnector(
            limit=self.max_concurrent,  # 连接池大小
            ttl_dns_cache=300,  # DNS缓存时间
            keepalive_timeout=30,  # 连接保持超时
            force_close=False,  # 避免与keepalive_timeout冲突
            enable_cleanup_closed=True,  # 启用已关闭连接的清理
            use_dns_cache=True  # 启用DNS缓存
        )
        timeout = aiohttp.ClientTimeout(
            total=60,  # 总超时时间
            connect=30,  # 连接超时时间
            sock_read=30,  # 读取超时时间
            sock_connect=30,  # socket连接超时时间
        )
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            connector_owner=True,  # 会话拥有连接器，会自动关闭
            raise_for_status=False  # 不自动抛出状态码异常，手动处理
        )

        # 并发控制优化：拆分 HTTP/WS 并发，避免等待 WS 接收时占用全局并发槽
        http_limit = int(self.config.get("http_concurrency", self.max_concurrent))
        ws_limit = int(self.config.get("ws_concurrency", self.max_concurrent))
        http_limit = max(1, min(http_limit, self.max_concurrent))
        ws_limit = max(1, min(ws_limit, self.max_concurrent))

        self.semaphore = asyncio.Semaphore(self.max_concurrent)  # 兼容保留
        self.http_semaphore = asyncio.Semaphore(http_limit)
        self.ws_semaphore = asyncio.Semaphore(ws_limit)

    async def generate_header(self):
        """生成每个账号的 headers"""
        try:
            # 避免重复调用时 headers 叠加
            self.accounts_headers = []

            tokens_str = OperateConfig().get_ini_value("PARAMS", "tokens")
            self.tokens = ast.literal_eval(tokens_str)
            for account, token in self.tokens:
                headers = {
                    "Access-Token": token,
                    "Content-Type": "application/x-www-form-urlencoded"
                }
                self.accounts_headers.append((str(account), headers))
            return self.accounts_headers
        except KeyboardInterrupt:
            logger.log_out("info", "用户中断程序执行")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.log_out("error", f"生成请求头失败: {str(e)}")
            return []

    async def get_order_ids(self, account_name: str, headers: Dict[str, str]) -> List[str]:
        """获取账户下所有持仓订单的订单ID"""
        if not self.session:
            logger.log_out("error", f"账户{account_name} aiohttp会话未初始化")
            return []

        sem = self.http_semaphore or self.semaphore
        try:
            async with sem:
                async with self.session.get(self.hold_url, headers=headers) as response:
                    if response.status != 200:
                        logger.log_out("warning", f"账户{account_name}获取订单ID请求失败，HTTP状态码: {response.status}")
                        return []

                    # 小优化：避免 content-type 严格校验带来的额外开销/异常
                    data = await response.json(content_type=None)
                    if data.get("code") != "OK":
                        logger.log_out("warning", f"账户{account_name}获取订单ID失败，返回码: {data.get('code')}")
                        return []

                    all_order_ids = [
                        order["orderId"]
                        for symbol_data in data.get("data", [])
                        for order in symbol_data.get("orderList", [])
                    ]
                    # 高频日志会显著拖慢总耗时，如需可在 logger 配置控制级别
                    logger.log_out("info", f"账户{account_name}的持仓订单ID有: {all_order_ids}")
                    return all_order_ids
        except aiohttp.ClientError as e:
            logger.log_out("error", f"账户{account_name}获取订单ID网络错误: {str(e)}")
        except json.JSONDecodeError as e:
            logger.log_out("error", f"账户{account_name}获取订单ID响应解析错误: {str(e)}")
        except Exception as e:
            logger.log_out("error", f"账户{account_name}获取订单ID发生意外错误: {str(e)}")
        return []

    async def send_close_orders_request(self, account_name: str, headers: Dict[str, str], all_order_ids: List[str]):
        """发送平仓请求"""
        if not all_order_ids:
            logger.log_out("info", f"账户{account_name}持仓订单为零，不执行平仓操作")
            return

        max_retries = int(self.config.get("max_retries", 2))
        retry_delay = float(self.config.get("retry_delay", 0.5))  # 提速：默认缩短重试等待
        open_timeout = float(self.config.get("open_timeout", 3.0))  # 提速：连接失败更快返回

        # 提速关键点：不要在占用并发槽时等待接收超时
        sem = self.ws_semaphore or self.semaphore

        for attempt in range(max_retries):
            websocket = None
            receive_task = None
            try:
                async with sem:
                    websocket = await websockets.connect(
                        self.ws_url,
                        extra_headers=headers,
                        close_timeout=5,
                        ping_interval=30,
                        ping_timeout=10,
                        open_timeout=open_timeout,
                    )
                    logger.log_out("info", f"账户 {account_name} WebSocket 连接成功 (尝试 {attempt + 1}/{max_retries})")

                    timeout_duration = float(self.config.get("receive_timeout", 1.0))

                    async def receive_messages(ws, acc_name):
                        message_count = 0
                        recv = ws.recv
                        loads = json.loads
                        log_out = logger.log_out
                        try:
                            while True:
                                try:
                                    message = await asyncio.wait_for(recv(), timeout=timeout_duration)
                                    try:
                                        message_data = loads(message)
                                    except json.JSONDecodeError as err:
                                        log_out("error", f"账户 {acc_name} 消息解析失败: {str(err)}")
                                        continue
                                    message_count += 1
                                    if message_data.get("eventType") == "marketOrder" and message_data.get("eventData"):
                                        log_out("info", f"账户 {acc_name} 平仓结果: {message_data.get('eventData')}")
                                except asyncio.TimeoutError:
                                    break
                                except websockets.exceptions.WebSocketException:
                                    break
                        except Exception as e:
                            log_out("error", f"账户 {acc_name} 接收消息时发生错误: {str(e)}")
                        return message_count

                    receive_task = asyncio.create_task(receive_messages(websocket, account_name))

                    # 批量发送平仓请求（优化：局部绑定减少属性查找）
                    send = websocket.send
                    dumps = json.dumps
                    success_count = 0
                    for order_id in all_order_ids:
                        close_data = {
                            "eventType": "marketOrder",
                            "eventData": {
                                "action": "close",
                                "orderFrom": "SELF",
                                "orderId": order_id
                            }
                        }
                        await send(dumps(close_data, ensure_ascii=False))
                        success_count += 1

                    logger.log_out("info", f"账户 {account_name} 共发送 {success_count}/{len(all_order_ids)} 个平仓请求")

                # 关键优化：接收等待放到 semaphore 之外，避免占用并发槽
                if receive_task:
                    await receive_task

                break

            except websockets.exceptions.WebSocketException as errorf:
                logger.log_out("error",
                               f"账号 {account_name} - WebSocket 连接失败 (尝试 {attempt + 1}/{max_retries}): {str(errorf)}")
                if attempt < max_retries - 1 and retry_delay > 0:
                    await asyncio.sleep(retry_delay * (attempt + 1))
            except asyncio.CancelledError:
                logger.log_out("info", f"账号 {account_name} - 任务被取消")
                raise
            except Exception as errorg:
                logger.log_out("error",
                               f"账号 {account_name} - 发生未预期的错误 (尝试 {attempt + 1}/{max_retries}): {str(errorg)}")
                if attempt < max_retries - 1 and retry_delay > 0:
                    await asyncio.sleep(retry_delay * (attempt + 1))
            finally:
                if receive_task and not receive_task.done():
                    receive_task.cancel()
                    try:
                        await receive_task
                    except asyncio.CancelledError:
                        pass
                if websocket and not websocket.closed:
                    try:
                        await websocket.close()
                    except Exception:
                        pass

    async def process_single_account(self, account_name: str, headers: Dict[str, str]):
        """处理单个账户的完整流程"""
        all_order_ids = await self.get_order_ids(account_name, headers)
        if all_order_ids:
            await self.send_close_orders_request(account_name, headers, all_order_ids)

    async def send_subscribe_request(self):
        """为所有用户发送平仓请求"""
        HEADERS = await self.generate_header()
        if not HEADERS:
            logger.log_out("error", "无法获取用户请求头，跳过交易请求")
            return

        total_accounts = len(HEADERS)
        logger.log_out("info", f"开始处理 {total_accounts} 个账号，最大并发数: {self.max_concurrent}")

        # 提速/省内存：分块调度任务，避免一次性创建过多 task
        chunk_size = int(self.config.get("task_chunk_size", max(1000, self.max_concurrent * 2)))
        chunk_size = max(1, chunk_size)

        idx = 0
        success_count_total = 0
        failure_count_total = 0

        while idx < total_accounts:
            sub = HEADERS[idx: idx + chunk_size]
            tasks = [asyncio.create_task(self.process_single_account(account_name, headers)) for account_name, headers in sub]

            try:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                success_count = sum(1 for r in results if not isinstance(r, Exception))
                failure_count = len(results) - success_count
                success_count_total += success_count
                failure_count_total += failure_count

                for i, r in enumerate(results):
                    if isinstance(r, Exception):
                        account_name = sub[i][0]
                        logger.log_out("error", f"账号 {account_name} 执行失败: {str(r)}")
            finally:
                for t in tasks:
                    if not t.done():
                        t.cancel()
                        try:
                            await t
                        except asyncio.CancelledError:
                            pass

            idx += chunk_size

        logger.log_out("info", f"所有账号处理完成: 成功 {success_count_total} 个，失败 {failure_count_total} 个")
        logger.log_out("info", f"所有 {total_accounts} 个账号的平仓请求处理完成")

    async def close(self):
        """清理资源的方法"""
        if self.session and not self.session.closed:
            try:
                await self.session.close()
                logger.log_out("info", "aiohttp会话已关闭")
            except RuntimeError as e:
                if "Event loop is closed" in str(e):
                    if hasattr(self.session, "_connector") and hasattr(self.session._connector, "close"):
                        self.session._connector.close()
                        logger.log_out("info", "aiohttp连接池已同步关闭")
                else:
                    logger.log_out("warning", f"关闭会话时发生错误: {str(e)}")
            except Exception as e:
                logger.log_out("warning", f"关闭会话时发生错误: {str(e)}")
        self.session = None


async def main():
    async with CloseOrderMore(max_concurrent=1000) as close_order:
        try:
            logger.log_out("info", "开始 WebSocket 发送请求...")
            await close_order.send_subscribe_request()
        except KeyboardInterrupt:
            logger.log_out("info", "用户中断程序执行")
        except asyncio.CancelledError:
            logger.log_out("info", "任务被取消")
        except Exception as error:
            logger.log_out("error", f"程序执行失败: {str(error)}")


if __name__ == "__main__":
    setup_event_loop_policy()
    asyncio.run(main())
