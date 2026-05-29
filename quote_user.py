import ast
import json
import time
import ssl
from threading import Lock

import gevent
import websocket
from locust import User, between, events, task

from Analyze_Quote_Data import analyze_quote
from MT.Flopotech.BaseMethod.log_module import logger
from MT.Flopotech.BaseMethod.operate_config import OperateConfig
from MT.Flopotech.Config.More_Account import WEBSOCKET_PUBLIC_CONFIG


class QuoteUser(User):
    """Locust 用户类：连接成功后持续接收行情，停止时再关闭连接"""

    wait_time = between(0.1, 0.5)
    tokens = []
    url = WEBSOCKET_PUBLIC_CONFIG["websocket_url"]
    quote_data = {
        "eventType": "subscribe",
        "eventData": {"subList": [{"channel": "quote", "symbol": "BTCUSD"}]},
    }
    token_index = 0
    _token_lock = Lock()

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

    def __init__(self, environment):
        super().__init__(environment)
        self.headers = None
        self.account_name = None
        self.websocket = None
        self._running = False
        self._receiver_greenlet = None

    def _is_ws_connected(self):
        return self.websocket is not None and getattr(self.websocket, "connected", False)

    def _close_ws(self):
        if self.websocket is not None:
            try:
                self.websocket.close()
            except Exception:
                pass
            finally:
                self.websocket = None

    def _connect_and_subscribe(self):
        """建立连接并发送订阅；成功后保持连接不关闭"""
        if self._is_ws_connected():
            return True

        if not self.headers:
            token_info = self.__class__._get_next_token()
            if not token_info:
                logger.log_out("error", "[用户] 没有可用的账号，无法建立连接")
                return False
            account, token, _ = token_info
            self.account_name = str(account)
            self.headers = {"Access-Token": token}
            logger.log_out("info", f"[{self.account_name}] 延迟初始化完成")

        start_time = time.time()
        try:
            header_list = [f"{k}: {v}" for k, v in self.headers.items()]
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
        """后台持续接收行情；连接断开会自动重连"""
        while self._running:
            if not self._is_ws_connected():
                connected = self._connect_and_subscribe()
                if not connected:
                    gevent.sleep(1)
                    continue

            try:
                message = self.websocket.recv()
                self.process_message(message)
                gevent.sleep(0)
            except websocket.WebSocketTimeoutException:
                # 超时不视为失败，继续等待下一条消息
                continue
            except websocket.WebSocketConnectionClosedException:
                logger.log_out("warning", f"[{self.account_name}] 连接已关闭，准备重连")
                self._close_ws()
                gevent.sleep(1)
            except Exception as e:
                logger.log_out("error", f"[{self.account_name}] 接收消息异常: {type(e).__name__}: {e}")
                self._close_ws()
                gevent.sleep(1)

    def on_start(self):
        """每个用户启动时执行（实例方法）"""
        self.__class__._load_tokens()
        token_info = self.__class__._get_next_token()
        if token_info:
            account, token, _ = token_info
            self.account_name = str(account)
            self.headers = {"Access-Token": token}
            logger.log_out("info", f"[{self.account_name}] 初始化完成")
        else:
            logger.log_out("error", "没有可用的账号，用户将无法执行任务")

        self._running = True
        self._receiver_greenlet = gevent.spawn(self._receive_quotes_loop)

    @task(1)
    def subscribe_to_quotes(self):
        """保持任务活跃；连接建立后由后台 greenlet 持续接收数据"""
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
        """用户停止时才关闭 ws 连接"""
        self._running = False
        self._close_ws()
        if self._receiver_greenlet is not None and not self._receiver_greenlet.dead:
            self._receiver_greenlet.join(timeout=2)
            if not self._receiver_greenlet.dead:
                self._receiver_greenlet.kill()
        logger.log_out("info", f"[{self.account_name}] 用户停止，连接已关闭")


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """测试停止时执行最终统计"""
    logger.log_out("info", "测试停止，正在生成最终报告...")
    analyze_quote.quote_final_summary()
