# -*- coding: utf-8 -*-
"""pytest 公共夹具。

设计目标 —— 测试不该要求你手工先起一个 broker：
    * 默认：用 amqtt 在本进程内起 broker（测试结束自动销毁）
    * 备选：设置环境变量 V2X_BROKER=127.0.0.1:1883 指向外部 broker（如 Mosquitto）
"""

from __future__ import annotations

import os

import pytest

from v2x.broker import InProcessBroker


@pytest.fixture(scope="session")
def broker_addr():
    """返回 (host, port)。"""
    external = os.environ.get("V2X_BROKER")
    if external:
        host, _, port = external.partition(":")
        yield (host or "127.0.0.1", int(port or 1883))
        return

    broker = InProcessBroker()
    try:
        broker.start()
    except Exception as exc:
        pytest.skip(
            "内置 broker 不可用，可设置 V2X_BROKER=host:port 指向外部 broker（如 Mosquitto）。"
            f"原因：{exc}"
        )
    yield broker.addr
    broker.stop()
