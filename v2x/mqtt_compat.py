# -*- coding: utf-8 -*-
"""paho-mqtt 版本兼容层。

paho-mqtt 2.x 引入 CallbackAPIVersion，不再允许 `Client()` 空参调用；
1.x 则没有这个枚举。这里统一收口，避免每个文件都写 try/except。
"""

from __future__ import annotations


def new_client(client_id: str):
    """创建一个 MQTT 客户端，兼容 paho-mqtt 1.x 与 2.x。"""
    import paho.mqtt.client as mqtt  # 延迟导入：没装 paho 时不影响纯逻辑测试

    try:
        # paho >= 2.0
        return mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
    except (AttributeError, TypeError):
        # paho 1.x
        return mqtt.Client(client_id=client_id)


def on_connect_ok(*args, **kwargs) -> bool:
    """版本无关地判断"连接是否成功"。

    paho 1.x 回调签名: (client, userdata, flags, rc)
    paho 2.x 回调签名: (client, userdata, flags, reason_code, properties)
    用 *args 接住，只关心第 4 个位置参数。
    """
    if len(args) >= 4:
        code = args[3]
        # 2.x 传的是 ReasonCode 对象，1.x 传 int
        return getattr(code, "is_failure", None) is False or code in (0, "0")
    return True
