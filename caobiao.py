#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@Date   : 2026/1/15
@Author : Administrator
@Description : 获取测试账户所有持仓订单的订单ID

问题修复/优化点（相对原始代码）：
1) tokens 解析与遍历：原代码 `for account, token in self.tokens:` 在 tokens 为 dict 时会迭代 key 导致解包失败。
2) 返回值：原代码最后 `return {}` 导致主函数拿不到任何订单ID数据。
3) 成功/失败统计口径：把“请求成功但无订单”和“请求失败”区分开，避免 success_count 被误用。
4) 并发与任务调度：使用 `asyncio.as_completed` 流式回收结果，降低一次性 gather 的内存峰值。
5) 计时：使用 `asyncio.get_running_loop().time()`，避免 `get_event_loop()` 在新版本中的弃用/行为差异。
6) Session/Connector：补充 limit_per_host，并把超时/异常处理集中且更明确。
"""

import ast
import asyncio
import json
import platform
import sys
import warnings
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple, Union

import aiohttp

from MT.Flopotech.BaseMethod.log_module import logger
from MT.Flopotech.BaseMethod.operate_config import OperateConfig
from MT.Flopotech.Config.More_Account import APP_URL


def setup_event_loop_policy() -> None:
    """须在 asyncio.run 之前调用（适配 Windows）。"""
    if platform.system() == "Windows" and sys.version_info >= (3, 8):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    warnings.filterwarnings("ignore", category=ResourceWarning)
    warnings.filterwarnings("ignore", category=RuntimeWarning)


TokensType = Union[Dict[Union[str, int], str], List[Tuple[Union[str, int], str]], Tuple[Tuple[Union[str, int], str], ...]]


@dataclass(frozen=True)
class AccountResult:
    account_name: str
    order_ids: List[str]
    ok: bool  # 请求并解析成功（即使 order_ids 为空也算 ok）
    error: Optional[str] = None  # 失败原因（ok=False 时）


class GetOrderIds:
    """获取测试账户所有持仓订单的订单ID"""

    def __init__(self, max_concurrent: int = 200):
        """
        初始化GetOrderIds实例
        :param max_concurrent: 最大并发数（也是连接池/限流上限）
        """
        self.hold_url = f"{APP_URL}/api/ord/order/hold"
        self.tokens: TokensType = {}
        self.accounts_headers: Dict[str, Dict[str, str]] = {}
        self.order_ids: Dict[str, List[str]] = {}

        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore: Optional[asyncio.Semaphore] = None

        self.total_order_count: int = 0
        self.order_count_by_account: Dict[str, int] = {}
        self.accounts_without_orders: List[str] = []
        self.failed_accounts: Dict[str, str] = {}  # account -> reason

        # 注意：500 并发对服务端/网络压力很大，默认下调到 200；如需可在外部显式传入 500
        self.max_concurrent: int = int(max_concurrent)

    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            await self.close()
        except Exception as e:
            logger.log_out("warning", f"上下文管理器退出时关闭资源失败: {str(e)}")

    async def initialize(self) -> aiohttp.ClientSession:
        """初始化资源"""
        if not self.session or self.session.closed:
            connector = aiohttp.TCPConnector(
                limit=self.max_concurrent,
                limit_per_host=self.max_concurrent,
                ttl_dns_cache=300,
                keepalive_timeout=30,
                force_close=False,
                ssl=False,
                enable_cleanup_closed=True,
                use_dns_cache=True,
            )
            timeout = aiohttp.ClientTimeout(
                total=60,
                connect=30,
                sock_read=30,
                sock_connect=30,
            )
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                connector_owner=True,
                raise_for_status=False,
            )
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
        return self.session

    @staticmethod
    def _normalize_tokens(tokens: TokensType) -> Iterable[Tuple[str, str]]:
        """
        兼容 tokens 为 dict / list[tuple] / tuple[tuple] 的格式，统一输出 (account, token) 的字符串形式。
        """
        if isinstance(tokens, dict):
            for k, v in tokens.items():
                yield str(k), str(v)
            return
        if isinstance(tokens, (list, tuple)):
            for item in tokens:
                if not isinstance(item, (list, tuple)) or len(item) != 2:
                    continue
                yield str(item[0]), str(item[1])
            return

    async def generate_header(self) -> List[Tuple[str, Dict[str, str]]]:
        """生成每个账号的 headers"""
        try:
            tokens_str = OperateConfig().get_ini_value("PARAMS", "tokens")
            self.tokens = ast.literal_eval(tokens_str)

            self.accounts_headers = {}
            for account, token in self._normalize_tokens(self.tokens):
                if not token:
                    continue
                # GET 请求不需要 Content-Type；这里保留也无害，但更推荐只传必要字段
                self.accounts_headers[account] = {
                    "Access-Token": token,
                }
            return list(self.accounts_headers.items())
        except KeyboardInterrupt:
            logger.log_out("info", "用户中断程序执行")
            return []
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.log_out("error", f"生成请求头失败: {str(e)}")
            return []

    async def _get_single_account_order_ids(self, account_name: str, headers: Dict[str, str]) -> AccountResult:
        """获取单个账户的订单ID"""
        if not self.session or self.session.closed or not self.semaphore:
            return AccountResult(account_name=account_name, order_ids=[], ok=False, error="session未初始化")

        try:
            async with self.semaphore:
                request_timeout = aiohttp.ClientTimeout(total=45)
                async with self.session.get(self.hold_url, headers=headers, timeout=request_timeout) as response:
                    if response.status != 200:
                        return AccountResult(
                            account_name=account_name,
                            order_ids=[],
                            ok=False,
                            error=f"HTTP状态码: {response.status}",
                        )

                    try:
                        data = await response.json(content_type=None)
                    except (json.JSONDecodeError, aiohttp.ContentTypeError) as e:
                        return AccountResult(account_name=account_name, order_ids=[], ok=False, error=f"响应解析失败: {str(e)}")

                    if data.get("code") != "OK":
                        return AccountResult(
                            account_name=account_name,
                            order_ids=[],
                            ok=False,
                            error=f"返回码: {data.get('code', '未知')}",
                        )

                    account_order_ids: List[str] = []
                    for symbol_data in data.get("data", []) or []:
                        for order in (symbol_data.get("orderList", []) or []):
                            order_id = order.get("orderId")
                            if order_id is not None:
                                account_order_ids.append(str(order_id))

                    if account_order_ids:
                        logger.log_out("info", f"账户{account_name}获取到 {len(account_order_ids)} 个订单ID")

                    return AccountResult(account_name=account_name, order_ids=account_order_ids, ok=True)
        except asyncio.TimeoutError:
            return AccountResult(account_name=account_name, order_ids=[], ok=False, error="请求超时")
        except asyncio.CancelledError:
            logger.log_out("warning", f"账户{account_name}获取订单ID任务被取消")
            raise
        except aiohttp.ClientError as e:
            return AccountResult(account_name=account_name, order_ids=[], ok=False, error=f"网络错误: {str(e)}")
        except Exception as e:
            return AccountResult(account_name=account_name, order_ids=[], ok=False, error=f"意外错误: {str(e)}")

    async def get_order_ids(self) -> Dict[str, List[str]]:
        """获取所有账户的订单ID（返回 account -> orderId 列表）"""
        accounts_headers = await self.generate_header()
        if not accounts_headers:
            logger.log_out("error", "没有获取到任何账户信息")
            return {}

        logger.log_out("info", f"开始并发获取 {len(accounts_headers)} 个账户的订单ID...")
        logger.log_out("info", f"最大并发数: {self.max_concurrent}")

        # 重置统计
        self.total_order_count = 0
        self.order_count_by_account = {}
        self.order_ids = {}
        self.accounts_without_orders = []
        self.failed_accounts = {}

        tasks = [asyncio.create_task(self._get_single_account_order_ids(name, hdr)) for name, hdr in accounts_headers]

        ok_count = 0
        fail_count = 0

        # 关键修复：
        # Windows + aiohttp(aiohappyeyeballs) 下，如果外部取消/异常导致提前退出，
        # 必须在 finally 中 cancel + await 回收所有未完成任务，否则会出现：
        # "Task was destroyed but it is pending!"
        try:
            # 流式回收结果，避免一次性 gather 占用更大内存
            for fut in asyncio.as_completed(tasks):
                result = await fut
                self.order_ids[result.account_name] = result.order_ids
                self.order_count_by_account[result.account_name] = len(result.order_ids)
                self.total_order_count += len(result.order_ids)

                if result.ok:
                    ok_count += 1
                    if not result.order_ids:
                        self.accounts_without_orders.append(result.account_name)
                else:
                    fail_count += 1
                    self.failed_accounts[result.account_name] = result.error or "未知原因"
        except asyncio.CancelledError:
            logger.log_out("warning", "get_order_ids 被取消，正在回收未完成任务...")
            raise
        finally:
            pending = [t for t in tasks if not t.done()]
            if pending:
                for t in pending:
                    t.cancel()
                # 必须 await，确保所有取消传播完成，避免事件循环关闭时出现 pending task 告警
                await asyncio.gather(*pending, return_exceptions=True)

        # 统计输出
        logger.log_out("info", "=" * 50)
        logger.log_out("info", "订单ID获取结果统计：")
        logger.log_out("info", f"总账户数：{len(accounts_headers)}")
        logger.log_out("info", f"请求成功账户数：{ok_count}")
        logger.log_out("info", f"请求失败账户数：{fail_count}")
        logger.log_out("info", f"总订单ID数：{self.total_order_count}")

        if self.accounts_without_orders:
            logger.log_out("info", f"无订单账户数：{len(self.accounts_without_orders)}")
            logger.log_out("info", f"无订单账户列表：{self.accounts_without_orders}")

        if self.failed_accounts:
            logger.log_out("info", f"失败账户明细：{self.failed_accounts}")

        logger.log_out("info", "=" * 50)
        return self.order_ids

    async def close(self) -> None:
        """清理资源，避免事件循环关闭错误"""
        if self.session and not self.session.closed:
            try:
                await self.session.close()
                # 给事件循环一个机会处理 connector 内部取消（Windows 下更常见）
                await asyncio.sleep(0)
                logger.log_out("info", "aiohttp会话已关闭")
            except RuntimeError as e:
                if "Event loop is closed" in str(e):
                    try:
                        if getattr(self.session, "_connector", None) is not None:
                            self.session._connector.close()
                            logger.log_out("info", "aiohttp连接池已同步关闭")
                    except Exception as sync_e:
                        logger.log_out("warning", f"同步关闭连接池失败: {str(sync_e)}")
                else:
                    logger.log_out("warning", f"关闭会话时发生错误: {str(e)}")
            except Exception as e:
                logger.log_out("warning", f"关闭会话时发生错误: {str(e)}")
        self.session = None


async def main() -> None:
    """主函数"""
    try:
        async with GetOrderIds(max_concurrent=500) as getter:
            logger.log_out("info", "开始获取所有账户的订单ID...")
            loop = asyncio.get_running_loop()
            start_time = loop.time()

            order_ids = await getter.get_order_ids()

            total_time = loop.time() - start_time
            logger.log_out("info", f"所有任务完成，总耗时: {total_time:.2f} 秒")
            logger.log_out("info", f"获取结果: {order_ids}")

            if getter.accounts_without_orders:
                logger.log_out("info", "=" * 50)
                logger.log_out("info", "【没有订单的账号统计】")
                logger.log_out("info", f"没有订单的账号数量: {len(getter.accounts_without_orders)}")
                logger.log_out("info", f"没有订单的账号列表: {getter.accounts_without_orders}")
                logger.log_out("info", "=" * 50)
    except KeyboardInterrupt:
        logger.log_out("info", "用户中断程序执行")
    except asyncio.CancelledError:
        logger.log_out("info", "任务被取消")
    except RuntimeError as e:
        if "Event loop is closed" in str(e):
            logger.log_out("error", f"事件循环已关闭: {str(e)}")
        else:
            logger.log_out("error", f"程序执行失败: {str(e)}")
    except Exception as e:
        logger.log_out("error", f"程序执行失败: {str(e)}")


if __name__ == "__main__":
    setup_event_loop_policy()
    try:
        asyncio.run(main())
    except RuntimeError as e:
        if "Event loop is closed" not in str(e):
            logger.log_out("error", f"主程序执行失败: {str(e)}")
    except Exception as e:
        logger.log_out("error", f"主程序执行失败: {str(e)}")
