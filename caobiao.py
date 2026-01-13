import ast
import asyncio
import json
import platform
import sys
import warnings
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    import websockets
except Exception:  # pragma: no cover
    websockets = None  # type: ignore[assignment]

try:
    from MT.Flopotech.BaseMethod.log_module import logger  # type: ignore
except Exception:  # pragma: no cover
    class _FallbackLogger:
        def log_out(self, level: str, msg: str) -> None:
            print(f"[{level}] {msg}")

    logger = _FallbackLogger()  # type: ignore[assignment]

try:
    from MT.Flopotech.Config.More_Account import WEBSOCKET_PRIVATE_CONFIG  # type: ignore
except Exception:  # pragma: no cover
    WEBSOCKET_PRIVATE_CONFIG = {
        "client_id": "0",
        "websocket_url": "",
        "receive_timeout": 30,
    }

try:
    from MT.Flopotech.BaseMethod.operate_config import OperateConfig  # type: ignore
except Exception:  # pragma: no cover
    class OperateConfig:  # type: ignore[override]
        def get_ini_value(self, section: str, key: str) -> Optional[str]:
            # 兜底：允许通过环境变量注入
            # e.g. TOKENS='[["acc1","token1"],["acc2","token2"]]'
            import os

            if section == "PARAMS" and key == "tokens":
                return os.environ.get("TOKENS")
            return None


def setup_event_loop_policy() -> None:
    """Windows系统事件循环兼容性处理 - 必须在 asyncio.run 之前设置。"""
    if platform.system() == "Windows" and sys.version_info >= (3, 8):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    # 抑制某些环境下的资源/运行时告警噪声
    warnings.filterwarnings("ignore", category=ResourceWarning)
    warnings.filterwarnings("ignore", category=RuntimeWarning)


def _parse_tokens(raw: Any) -> List[Tuple[str, str]]:
    """
    将 tokens 解析为 List[(account, token)] 的统一结构。
    兼容：
    - dict: {account: token}
    - list/tuple: [(account, token), ...]
    - list/tuple: [{"account": "...", "token": "..."}, ...]
    """
    if raw is None:
        return []

    if isinstance(raw, dict):
        return [(str(k), str(v)) for k, v in raw.items()]

    if isinstance(raw, (list, tuple)):
        out: List[Tuple[str, str]] = []
        for item in raw:
            if isinstance(item, dict):
                if "account" in item and "token" in item:
                    out.append((str(item["account"]), str(item["token"])))
                continue
            if isinstance(item, (list, tuple)) and len(item) == 2:
                out.append((str(item[0]), str(item[1])))
        return out

    return []


class MarketOrder:
    """市价单开仓"""

    def __init__(self) -> None:
        self.config: Dict[str, Any] = dict(WEBSOCKET_PRIVATE_CONFIG)
        self.tokens: List[Tuple[str, str]] = []
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

    async def generate_header(self) -> List[Tuple[str, Dict[str, str]]]:
        """生成所有账号的 headers"""
        try:
            tokens_str = OperateConfig().get_ini_value("PARAMS", "tokens")
            if not tokens_str:
                logger.log_out("error", "未读取到 tokens 配置(PARAMS.tokens)")
                return []

            parsed: Any = None
            # 优先按 JSON 解析，其次再走 literal_eval
            try:
                parsed = json.loads(tokens_str)
            except Exception:
                parsed = ast.literal_eval(tokens_str)

            self.tokens = _parse_tokens(parsed)
            if not self.tokens:
                logger.log_out("error", "tokens 解析为空或格式不支持")
                return []

            headers_list: List[Tuple[str, Dict[str, str]]] = []
            client_id = str(self.config.get("client_id", ""))
            for account, token in self.tokens:
                headers_list.append(
                    (
                        str(account),
                        {
                            "Access-Token": str(token),
                            "Client-ID": client_id,
                        },
                    )
                )

            # 去重（按账号名，保留最后一个 token）
            dedup: Dict[str, Dict[str, str]] = {}
            for account, headers in headers_list:
                dedup[account] = headers
            self.accounts_headers = list(dedup.items())
            return self.accounts_headers

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.log_out("error", f"生成请求头失败: {e!r}")
            return []

    async def _ws_connect(self, url: str, headers: Dict[str, str]):
        if websockets is None:  # pragma: no cover
            raise RuntimeError("缺少依赖 websockets，请先安装: pip install websockets")

        connect_kwargs = dict(
            close_timeout=10,
            ping_interval=30,
            ping_timeout=10,
        )

        # websockets 不同版本参数名不同：extra_headers / additional_headers
        try:
            return await websockets.connect(url, additional_headers=headers, **connect_kwargs)  # type: ignore[arg-type]
        except TypeError:
            return await websockets.connect(url, extra_headers=headers, **connect_kwargs)  # type: ignore[arg-type]

    async def send_trading_request(self, account_name: str, headers: Dict[str, str]) -> None:
        """发送交易请求"""
        websocket = None
        max_retries = 2
        retry_delay = 1.0

        url = str(self.config.get("websocket_url", "")).strip()
        if not url:
            logger.log_out("error", f"账号 {account_name} - websocket_url 为空，无法连接")
            return

        timeout_duration = float(self.config.get("receive_timeout", 30))

        for attempt in range(max_retries):
            try:
                websocket = await self._ws_connect(url, headers)
                logger.log_out("info", f"账号 {account_name} - WebSocket 连接成功")

                request_message = json.dumps(self.order_data, ensure_ascii=False)
                await websocket.send(request_message)
                logger.log_out("info", f"账号 {account_name} - 已发送交易请求: {self.order_data['eventData']}")

                message_count = 0
                while True:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=timeout_duration)
                        message_data = json.loads(message)
                        message_count += 1

                        if message_data.get("eventType") == "marketOrder":
                            logger.log_out("info", f"账号 {account_name} - 收到交易响应: {message_data}")
                        else:
                            logger.log_out("debug", f"账号 {account_name} - 收到消息: {message_data}")

                    except asyncio.TimeoutError:
                        logger.log_out("info", f"账号 {account_name} - 接收消息完成，共接收 {message_count} 条消息")
                        break
                    except json.JSONDecodeError as err:
                        logger.log_out("error", f"账号 {account_name} - 消息解析失败: {err!r}")
                        continue
                    except Exception as err:
                        # 连接被关闭等情况在不同 websockets 版本里可能抛不同异常，这里统一当作“本次结束”
                        logger.log_out("warning", f"账号 {account_name} - 接收中断: {err!r}")
                        break

                return  # 成功完成本次流程，不再重试

            except asyncio.CancelledError:
                raise
            except Exception as err:
                logger.log_out("error", f"账号 {account_name} - WebSocket 操作失败(尝试 {attempt + 1}/{max_retries}): {err!r}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (attempt + 1))
                else:
                    logger.log_out("error", f"账号 {account_name} - 已达到最大重试次数，放弃")

            finally:
                if websocket is not None:
                    try:
                        await websocket.close()
                        logger.log_out("info", f"账号 {account_name} - WebSocket 连接已关闭")
                    except Exception as err:
                        logger.log_out("warning", f"账号 {account_name} - 关闭 WebSocket 连接时出错: {err!r}")
                    websocket = None

    async def send_subscribe_request(self, batch_size: int = 100, pause_seconds: float = 5.0) -> None:
        """为每个用户的header分别发送交易请求（分批并发）"""
        headers_list = await self.generate_header()
        if not headers_list:
            logger.log_out("error", "无法获取用户请求头，跳过交易请求")
            return

        if batch_size <= 0:
            batch_size = 1

        total_accounts = len(headers_list)

        for batch_start in range(0, total_accounts, batch_size):
            batch_end = min(batch_start + batch_size, total_accounts)
            current_batch = headers_list[batch_start:batch_end]
            batch_no = batch_start // batch_size + 1

            logger.log_out("info", f"开始处理第 {batch_no} 批账号，共 {len(current_batch)} 个账号")

            tasks: List[asyncio.Task] = []
            task_to_account: Dict[asyncio.Task, str] = {}

            try:
                for account_name, headers in current_batch:
                    logger.log_out("info", f"开始发送交易请求,账号:{account_name} ...")
                    t = asyncio.create_task(self.send_trading_request(account_name, headers))
                    tasks.append(t)
                    task_to_account[t] = account_name

                if tasks:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for t, result in zip(tasks, results):
                        if isinstance(result, BaseException):
                            account_name = task_to_account.get(t, "<unknown>")
                            logger.log_out("error", f"第 {batch_no} 批 - 账号 {account_name} 执行失败: {result!r}")

                    logger.log_out("info", f"第 {batch_no} 批账号的交易请求发送完成")

            except asyncio.CancelledError:
                raise
            finally:
                pending = [t for t in tasks if not t.done()]
                for t in pending:
                    t.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)

            if batch_end < total_accounts and pause_seconds > 0:
                logger.log_out("info", f"等待{pause_seconds}秒后处理下一批账号...")
                await asyncio.sleep(pause_seconds)

        logger.log_out("info", "所有批次的账号交易请求发送完成")

    async def close(self) -> None:
        """清理资源的方法（预留）"""
        await asyncio.sleep(0.1)
        logger.log_out("info", "资源清理完成")


async def main() -> None:
    market_order = MarketOrder()
    try:
        logger.log_out("info", "开始 WebSocket 发送请求...")
        await market_order.send_subscribe_request()
    finally:
        await market_order.close()


if __name__ == "__main__":
    setup_event_loop_policy()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.log_out("info", "用户中断程序执行")
    except Exception as err:
        logger.log_out("error", f"程序执行失败: {err!r}")
