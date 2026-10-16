# -*- coding: utf-8 -*-
"""内置 MQTT broker（基于 amqtt），供测试与压测脚本复用。

不依赖 pytest —— 所以 tools/load_test.py 也能直接用。

【实现要点 / 踩过的坑 —— 这段值得看，别人基本都会踩】
amqtt 的 `Broker.__init__` 里有一行：

    self._loop = loop or asyncio.get_running_loop()

注意是 `get_running_loop()` 而不是 `get_event_loop()`：
它要求**此刻必须有一个正在运行的 loop**，光 `new_event_loop()` + `set_event_loop()`
是不够的，会直接抛 `RuntimeError: no running event loop`。

所以正确姿势是：在后台线程里创建 loop，然后**在一个协程内部**构造并启动 broker，
最后才 run_forever()。这样对 amqtt 0.10 / 0.11 / 0.12 都成立。
"""

from __future__ import annotations

import asyncio
import socket
import threading
import time
from typing import List, Optional, Tuple


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def wait_port(host: str, port: int, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.05)
    return False


def candidate_configs(port: int) -> List[dict]:
    """amqtt 各版本的 config 结构有差异，准备几套依次尝试（顺序 = 优先级）。

    第 1 套是 amqtt 0.11+ / 0.12 的推荐写法：显式声明 `plugins`。
    用它有两个好处：
      * 不再有 "sys_interval / auth / topic-check is deprecated" 告警
      * 不再从 EntryPoints 自动加载全部插件，从而消掉
        "Configuration parameter 'password-file' not found"
        "Loading plugins from EntryPoints is deprecated"
        这类噪声（以及 EventLogger / PacketLogger 的刷屏日志）
    后两套是给 0.10.x 等老版本兜底的旧写法。
    """
    listener = {"default": {"type": "tcp", "bind": f"127.0.0.1:{port}"}}
    return [
        # 0.11+ / 0.12 推荐写法
        {
            "listeners": listener,
            "plugins": {
                "amqtt.plugins.authentication.AnonymousAuthPlugin": {"allow_anonymous": True},
                "amqtt.plugins.sys.broker.BrokerSysPlugin": {"sys_interval": 0},
            },
        },
        # 旧写法（0.10.x 起可用）
        {
            "listeners": listener,
            "sys_interval": 0,
            "auth": {"allow-anonymous": True},
            "topic-check": {"enabled": False},
        },
        {
            "listeners": listener,
            "sys_interval": 0,
            "auth": {"allow-anonymous": True, "plugins": ["auth_anonymous"]},
            "topic-check": {"enabled": False, "plugins": ["topic_taboo"]},
        },
        # 兜底：只给 listeners
        {"listeners": listener},
    ]


class InProcessBroker:
    """在后台线程里跑一个 amqtt broker。"""

    def __init__(self, host: str = "127.0.0.1", port: Optional[int] = None) -> None:
        self.host = host
        self.port = port or free_port()
        self.errors: List[str] = []

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._broker = None
        self._thread: Optional[threading.Thread] = None
        self._ready = threading.Event()
        self._boot_error: Optional[str] = None

    # ------------------------------------------------------------ 线程内启动

    def _serve(self, cfg: dict) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop

        try:
            from amqtt.broker import Broker

            async def _boot():
                # 必须在"运行中的 loop"里构造 Broker，原因见模块 docstring
                broker = Broker(cfg)
                started = broker.start()
                if asyncio.iscoroutine(started):   # 0.11+ 是协程；老版本可能是同步的
                    await started
                return broker

            broker = loop.run_until_complete(_boot())
        except Exception as exc:
            self._boot_error = f"{type(exc).__name__}: {exc}"
            self._ready.set()
            try:
                loop.close()
            except Exception:
                pass
            self._loop = None
            return

        self._broker = broker
        self._ready.set()

        try:
            loop.run_forever()
        finally:
            try:
                loop.close()
            except Exception:
                pass

    # ------------------------------------------------------------ 对外接口

    def start(self, ready_timeout: float = 5.0) -> "InProcessBroker":
        try:
            import amqtt  # noqa: F401
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("未安装 amqtt：pip install -r requirements.txt") from exc

        for idx, cfg in enumerate(candidate_configs(self.port)):
            self._ready = threading.Event()
            self._boot_error = None
            self._loop = None
            self._broker = None

            self._thread = threading.Thread(
                target=self._serve, args=(cfg,), name=f"amqtt-broker-{idx}", daemon=True
            )
            self._thread.start()

            if not self._ready.wait(ready_timeout):
                self.errors.append(f"配置#{idx}: {ready_timeout}s 内未完成启动")
                self.stop()
                continue

            if self._boot_error:
                self.errors.append(f"配置#{idx} 启动失败: {self._boot_error}")
                self.stop()
                continue

            if wait_port(self.host, self.port, ready_timeout):
                return self

            self.errors.append(f"配置#{idx}: broker 已启动但端口 {self.port} 不可连")
            self.stop()

        raise RuntimeError(
            "内置 broker 启动失败（已尝试 %d 套配置）：\n  %s"
            % (len(self.errors), "\n  ".join(self.errors))
        )

    def stop(self, timeout: float = 5.0) -> None:
        if self._loop is not None and self._broker is not None:
            try:
                fut = asyncio.run_coroutine_threadsafe(self._broker.shutdown(), self._loop)
                fut.result(timeout=timeout)
            except Exception:
                pass
        if self._loop is not None:
            try:
                self._loop.call_soon_threadsafe(self._loop.stop)
            except Exception:
                pass
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None
        self._loop = None
        self._broker = None
        self._ready = threading.Event()

    @property
    def addr(self) -> Tuple[str, int]:
        return (self.host, self.port)

    def __enter__(self) -> "InProcessBroker":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()
