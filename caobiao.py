#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
 @ Date   : 2026/5/20
 @ Author : Administrator
 @ Description : WebSocket 长连接保持并发压力测试
"""
import ast
import json
import ssl
import time
import warnings
from threading import Lock

import gevent
import websocket
from gevent.lock import Semaphore
from locust import User, between, events, task

from MT.Flopotech.BaseMethod.operate_config import OperateConfig
from MT.Flopotech.Config.More_Account import WEBSOCKET_PRIVATE_CONFIG
from MT.Flopotech.OtherOne.log_module_01 import logger

warnings.filterwarnings("ignore", category=DeprecationWarning)


class MarketOrderUser(User):
    """Locust 用户类：单用户复用 WebSocket 长连接，仅保持连接不发送市价单"""

    wait_time = between(1, 3)

    # ---------- 类级共享 ----------
    tokens = []
    token_index = 0
    _token_lock = Lock()

    client_ids = WEBSOCKET_PRIVATE_CONFIG.get("client_ids", [])
    client_id_index = 0
    _client_id_lock = Lock()

    url = WEBSOCKET_PRIVATE_CONFIG["websocket_url"]

    # ---------- 可调参数 ----------
    connect_timeout = 10
    recv_timeout = 5
    reconnect_retry = 3
    heartbeat_interval = 20

    @classmethod
    def _load_tokens(cls):
        """加载账号 token（仅一次）"""
        if cls.tokens:
            return
        try:
            tokens_str = OperateConfig().get_ini_value(
                "Concurrent", "concurrentusers", "Concurrent_Users.ini"
            )
            if not tokens_str:
                raise ValueError("配置项 Concurrent.concurrentusers 为空")
            try:
                parsed = json.loads(tokens_str)
            except json.JSONDecodeError:
                parsed = ast.literal_eval(tokens_str)
            if not isinstance(parsed, list) or not parsed:
                raise ValueError("tokens 必须是非空 list")
            cls.tokens = parsed
            logger.log_out("info", f"已加载 {len(cls.tokens)} 个账号")
        except Exception as e:
            cls.tokens = []
            logger.log_out("error", f"加载 tokens 失败: {type(e).__name__}: {e}")

    @classmethod
    def _get_next_token(cls):
        if not cls.tokens:
            return None
        with cls._token_lock:
            idx = cls.token_index % len(cls.tokens)
            cls.token_index += 1
            return cls.tokens[idx]

    @classmethod
    def _get_next_client_id(cls):
        if not cls.client_ids:
            return None
        with cls._client_id_lock:
            idx = cls.client_id_index % len(cls.client_ids)
            cls.client_id_index += 1
            return cls.client_ids[idx]

    def __init__(self, environment):
        super().__init__(environment)
        self.account_name = None
        self.headers = None
        self.websocket = None
        self._ws_lock = Semaphore(1)
        self._running = False
        self._heartbeat_greenlet = None

    # ----------------- 连接管理 -----------------

    def _is_connected(self):
        return self.websocket is not None and getattr(self.websocket, "connected", False)

    def _close_ws(self):
        with self._ws_lock:
            if self.websocket is not None:
                try:
                    self.websocket.close()
                except Exception as e:
                    logger.log_out("error", f"[{self.account_name}] 关闭连接失败: {type(e).__name__}: {e}")
                finally:
                    self.websocket = None

    def _fire_request_event(self, name, start_time, exception=None):
        """上报到 Locust，用于统计 WebSocket 发出的请求数和失败数"""
        self.environment.events.request.fire(
            request_type="WebSocket",
            name=name,
            response_time=(time.perf_counter() - start_time) * 1000,
            response_length=0,
            exception=exception,
            context={"account": self.account_name},
        )

    def _connect_ws(self):
        """建立 WebSocket 长连接，成功返回 True"""
        if self._is_connected():
            return True
        if not self.headers:
            return False
        start_time = time.perf_counter()
        try:
            header_list = [f"{k}: {v}" for k, v in self.headers.items()]
            ws = websocket.create_connection(
                self.url,
                header=header_list,
                timeout=self.connect_timeout,
                sslopt={"cert_reqs": ssl.CERT_NONE, "check_hostname": False},
                enable_multithread=True,
            )
            ws.settimeout(self.recv_timeout)
            with self._ws_lock:
                self.websocket = ws
            logger.log_out("info", f"[{self.account_name}] WebSocket 长连接建立成功")
            self._fire_request_event("websocket_connect", start_time)
            return True
        except Exception as e:
            logger.log_out("error", f"[{self.account_name}] 建立连接失败: {type(e).__name__}: {e}")
            self._fire_request_event("websocket_connect", start_time, e)
            self._close_ws()
            return False

    def _ensure_connected(self):
        """确保连接可用，不可用时重连"""
        if self._is_connected():
            return True
        for i in range(1, self.reconnect_retry + 1):
            ok = self._connect_ws()
            if ok:
                return True
            logger.log_out("warning", f"[{self.account_name}] 重连失败({i}/{self.reconnect_retry})")
            gevent.sleep(min(2 * i, 5))
        return False

    def _heartbeat_loop(self):
        """后台心跳，保持长连接"""
        while self._running:
            gevent.sleep(self.heartbeat_interval)
            if not self._running:
                break
            if not self._is_connected():
                continue
            start_time = time.perf_counter()
            exc = None
            try:
                with self._ws_lock:
                    self.websocket.ping()
                logger.log_out("debug", f"[{self.account_name}] WebSocket 心跳成功")
            except Exception as e:
                exc = e
                logger.log_out("error", f"[{self.account_name}] 心跳失败: {type(e).__name__}: {e}")
                self._close_ws()
            finally:
                self._fire_request_event("websocket_ping", start_time, exc)

    # ----------------- Locust 生命周期 -----------------

    def on_start(self):
        self.__class__._load_tokens()
        token_info = self.__class__._get_next_token()
        if not token_info:
            logger.log_out("error", "没有可用账号，用户初始化失败")
            return

        client_id = self.__class__._get_next_client_id()
        if not client_id:
            logger.log_out("error", "没有可用 client_id，用户初始化失败")
            return

        account, token, _actid = token_info
        self.account_name = str(account)
        self.headers = {
            "Access-Token": token,
            "Client-ID": client_id,
        }
        self._running = True
        self._ensure_connected()
        self._heartbeat_greenlet = gevent.spawn(self._heartbeat_loop)
        logger.log_out("info", f"[{self.account_name}] 初始化完成，进入长连接保持模式")

    @task(1)
    def keep_websocket_connected(self):
        """保持 WebSocket 长连接，不发送业务请求"""
        if not self._running:
            gevent.sleep(1)
            return
        if not self._ensure_connected():
            logger.log_out("error", f"[{self.account_name}] WebSocket 不可用，重连失败")
        gevent.sleep(0)

    def on_stop(self):
        self._running = False
        if self._heartbeat_greenlet is not None and not self._heartbeat_greenlet.dead:
            self._heartbeat_greenlet.kill(block=False)
        self._close_ws()
        logger.log_out("info", f"[{self.account_name}] 用户停止，长连接已关闭")


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    logger.log_out("info", "测试停止，正在生成最终报告...")
    for request_name in ("websocket_connect", "websocket_ping"):
        stats = environment.stats.get(request_name, "WebSocket")
        logger.log_out(
            "info",
            f"{request_name} 请求数: {stats.num_requests}, 失败数: {stats.num_failures}",
        )
