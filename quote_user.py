import ast
import itertools
import json
import ssl
import time
from threading import Lock

import gevent
import websocket
from locust import User, between, events, task

from Analyze_Quote_Data import analyze_quote
from MT.Flopotech.BaseMethod.log_module import logger
from MT.Flopotech.BaseMethod.operate_config import OperateConfig
from MT.Flopotech.Config.More_Account import WEBSOCKET_PRIVATE_CONFIG, WEBSOCKET_PUBLIC_CONFIG


class QuoteUser(User):
    """Locust 用户类：连接成功后持续接收行情，不主动断开连接。"""

    wait_time = between(0.1, 0.5)
    tokens = []
    token_index = 0
    _token_lock = Lock()

    client_id_index = 0
    _client_id_lock = Lock()

    url = WEBSOCKET_PUBLIC_CONFIG["websocket_url"]
    quote_data = {
        "eventType": "subscribe",
        "eventData": {"subList": [{"channel": "quote", "symbol": "BTCUSD"}]},
    }

    @classmethod
    def _load_tokens(cls):
        """加载所有账号 token（类方法，仅执行一次）"""
        if cls.tokens:
            return

        config = OperateConfig()
        tokens_str = config.get_ini_value("PARAMS", "tokens")
        if not tokens_str:
            logger.log_out("error", "配置项 PARAMS.tokens 为空")
            cls.tokens = []
            return

        try:
            try:
                parsed = json.loads(tokens_str)
            except json.JSONDecodeError:
                parsed = ast.literal_eval(tokens_str)

            if not isinstance(parsed, list):
                raise ValueError("tokens 必须是列表类型")

            cls.tokens = parsed
            logger.log_out("info", f"已加载 {len(cls.tokens)} 个账号")
        except Exception as e:
            logger.log_out("error", f"加载 tokens 失败: {type(e).__name__}: {e}")
            cls.tokens = []

    @classmethod
    def _get_next_token(cls):
        """线程安全地轮询获取下一个 token"""
        if not cls.tokens:
            return None
        with cls._token_lock:
            idx = cls.token_index % len(cls.tokens)
            cls.token_index += 1
            return cls.tokens[idx]

    @classmethod
    def _get_next_client_id(cls):
        """线程安全轮询 clientID"""
        client_ids = WEBSOCKET_PRIVATE_CONFIG.get("client_ids", [])
        if not client_ids:
            return None
        with cls._client_id_lock:
            idx = cls.client_id_index % len(client_ids)
            cls.client_id_index += 1
            return client_ids[idx]

    def __init__(self, environment):
        super().__init__(environment)
        self.headers = None
        self.account_name = None
        self.websocket = None
        self._running = False
        self._receiver_greenlet = None
        # 保留 cycle，若配置运行时动态更新可快速切换 fallback。
        self.client_id_cycle = itertools.cycle(WEBSOCKET_PRIVATE_CONFIG.get("client_ids", []))

    def _is_ws_connected(self):
        return self.websocket is not None and getattr(self.websocket, "connected", False)

    def _close_ws(self):
        """仅在异常恢复场景清理失效句柄；不在停止流程主动断开。"""
        if self.websocket is not None:
            try:
                self.websocket.close()
            except Exception:
                pass
            finally:
                self.websocket = None

    def _build_headers(self):
        if self.headers:
            return self.headers

        token_info = self.__class__._get_next_token()
        if not token_info:
            logger.log_out("error", "[用户]没有可用的账号，无法建立连接")
            return None

        account, token, _ = token_info
        self.account_name = str(account)

        client_id = self.__class__._get_next_client_id()
        if client_id is None:
            # fallback：兼容部分环境只给了 cycle，不给静态列表
            try:
                client_id = next(self.client_id_cycle)
            except Exception:
                client_id = ""

        self.headers = {"accessToken": token, "clientID": client_id}
        logger.log_out("info", f"[{self.account_name}] 初始化完成")
        return self.headers

    def _connect_and_subscribe(self):
        """建立连接并发送订阅；连接成功后保持连接。"""
        if self._is_ws_connected():
            return True

        headers = self._build_headers()
        if not headers:
            return False

        start_time = time.time()
        try:
            header_list = [f"{k}: {v}" for k, v in headers.items()]
            ws = websocket.create_connection(
                self.url,
                header=header_list,
                timeout=10,
                sslopt={"cert_reqs": ssl.CERT_NONE, "check_hostname": False},
            )
            ws.settimeout(10.0)
            ws.send(json.dumps(self.quote_data, ensure_ascii=False))

            self.websocket = ws
            logger.log_out("info", f"[{self.account_name}] WebSocket 连接成功，已发送订阅请求")

            self.environment.events.request.fire(
                request_type="WebSocket",
                name="quote_subscribe",
                response_time=(time.time() - start_time) * 1000,
                response_length=0,
                exception=None,
                context={"account": self.account_name},
            )
            return True
        except Exception as e:
            self.environment.events.request.fire(
                request_type="WebSocket",
                name="quote_subscribe",
                response_time=(time.time() - start_time) * 1000,
                response_length=0,
                exception=e,
                context={"account": self.account_name},
            )
            logger.log_out("error", f"[{self.account_name}] 连接异常: {type(e).__name__}: {e}")
            self._close_ws()
            return False

    def _receive_quotes_loop(self):
        """后台持续接收行情；断连后自动重连并重发订阅。"""
        while self._running:
            if not self._is_ws_connected():
                if not self._connect_and_subscribe():
                    gevent.sleep(1)
                    continue

            try:
                message = self.websocket.recv()
                logger.log_out("debug", f"[{self.account_name}] 收到消息: {message}")
                self.process_message(message)
                gevent.sleep(0)
            except websocket.WebSocketTimeoutException:
                # 超时不主动断开，继续等待
                continue
            except websocket.WebSocketConnectionClosedException:
                logger.log_out("warning", f"[{self.account_name}] 连接被动关闭，准备重连")
                self.websocket = None
                gevent.sleep(1)
            except Exception as e:
                logger.log_out("error", f"[{self.account_name}] 接收消息异常: {type(e).__name__}: {e}")
                self.websocket = None
                gevent.sleep(1)

    def on_start(self):
        """每个用户启动时执行（实例方法）"""
        self.__class__._load_tokens()
        self._running = True
        self._receiver_greenlet = gevent.spawn(self._receive_quotes_loop)

    @task(1)
    def subscribe_to_quotes(self):
        """
        保持任务活跃：
        - 连接与接收由后台 greenlet 负责
        - 不在任务流程中主动断连
        """
        if self._receiver_greenlet is None or self._receiver_greenlet.dead:
            logger.log_out("warning", f"[{self.account_name}] 接收协程已退出，重新拉起")
            self._receiver_greenlet = gevent.spawn(self._receive_quotes_loop)
        gevent.sleep(1)

    def process_message(self, message: str):
        """处理接收到的消息"""
        try:
            parsed_message = json.loads(message)
            if parsed_message.get("channel") == "quote":
                analyze_quote.analyze_delay(parsed_message)
        except json.JSONDecodeError as errorj:
            logger.log_out("error", f"[{self.account_name}] JSON 解析错误: {errorj}")
        except Exception as errore:
            logger.log_out("error", f"[{self.account_name}] 处理消息时出错: {errore}")

    def on_stop(self):
        """
        用户停止时不主动断开 ws。
        仅停止接收循环，避免业务侧发送 close 帧。
        """
        self._running = False
        logger.log_out("info", f"[{self.account_name}] 用户停止，不主动关闭连接")


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """测试停止时执行最终统计（不主动关闭 ws）"""
    logger.log_out("info", "测试停止，正在生成最终报告...")
    analyze_quote.quote_final_summary()
