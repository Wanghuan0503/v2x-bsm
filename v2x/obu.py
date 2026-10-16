# -*- coding: utf-8 -*-
"""OBU 模拟器：以固定频率往云控平台上报 BSM。

对应真实场景里的"车端"——只是这里用一台 PC 上的多个线程代替多台车。
"""

from __future__ import annotations

import threading
import time
from typing import List, Tuple

from .bsm import MSG_CNT_MAX, encode, make_bsm, now_ms
from .mqtt_compat import new_client

DEFAULT_TOPIC_TEMPLATE = "v2x/obu/{obu_id}/bsm"


class ObuSimulator:
    """一个 OBU。要模拟 N 路并发就创建 N 个实例（或调 tools/load_test.py）。"""

    def __init__(
        self,
        host: str,
        port: int,
        obu_id: str = "OBU-0001",
        *,
        hz: float = 10.0,
        qos: int = 0,
        topic_template: str = DEFAULT_TOPIC_TEMPLATE,
        lat: float = 30.5231,
        lon: float = 114.3121,
        speed: float = 12.5,
        msg_cnt_start: int = 0,
    ) -> None:
        if hz <= 0:
            raise ValueError("hz 必须为正数")
        self.host = host
        self.port = port
        self.obu_id = obu_id
        self.hz = float(hz)
        self.qos = int(qos)
        self.topic = topic_template.format(obu_id=obu_id)
        self.lat, self.lon, self.speed = lat, lon, speed

        self.msg_cnt = int(msg_cnt_start)
        self.sent: List[Tuple[int, int]] = []   # [(sendTs, msgCnt), ...]
        self.publish_errors: List[str] = []

        self._client = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._lock = threading.Lock()

    # ------------------------------------------------------------ 生命周期

    def start(self, wait_connected: float = 10.0) -> "ObuSimulator":
        connected = threading.Event()

        self._client = new_client(self.obu_id)
        self._client.on_connect = lambda *a, **k: connected.set()
        self._client.connect(self.host, self.port, keepalive=30)
        self._client.loop_start()

        if not connected.wait(timeout=wait_connected):
            self._client.loop_stop()
            raise TimeoutError(f"{self.obu_id} 在 {wait_connected}s 内未连上 {self.host}:{self.port}")

        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name=self.obu_id, daemon=True)
        self._thread.start()
        return self

    def signal_stop(self) -> None:
        """只置停止标志并立即返回，不等线程结束。

        【为什么需要这个】多个 OBU 一起跑压测时，如果逐个调用 stop()，
        前一个在 join 期间（paho 的 loop_stop() 可能阻塞近 1 秒），
        后面的 OBU 还在继续上报 —— 结果每路 OBU 的统计时长都不一样，
        算出来的吞吐量会明显虚高。正确做法是先把所有 OBU 同时截止（本方法），
        再统一 close()。
        """
        self._stop.set()

    def close(self, timeout: float = 5.0) -> None:
        """等待上报线程退出并断开连接。可能阻塞（paho loop_stop 的特性）。"""
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None
        if self._client is not None:
            try:
                self._client.loop_stop()
                self._client.disconnect()
            except Exception:
                pass
            self._client = None

    def stop(self, timeout: float = 5.0) -> None:
        """单个 OBU 的便捷停止（= signal_stop + close）。"""
        self.signal_stop()
        self.close(timeout)

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def __enter__(self) -> "ObuSimulator":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()

    # ------------------------------------------------------------ 上报循环

    def _run(self) -> None:
        interval = 1.0 / self.hz
        next_at = time.perf_counter()

        while not self._stop.is_set():
            payload = make_bsm(
                self.obu_id,
                self.msg_cnt,
                lat=self.lat,
                lon=self.lon,
                speed=self.speed,
                send_ts=now_ms(),
            )
            info = self._client.publish(self.topic, encode(payload), qos=self.qos)
            if info.rc != 0:
                with self._lock:
                    self.publish_errors.append(f"publish rc={info.rc}")

            with self._lock:
                self.sent.append((payload["sendTs"], payload["msgCnt"]))
            # msgCnt 是 0..127 循环，越界要回绕——这正好对应真实标准的行为
            self.msg_cnt = (self.msg_cnt + 1) % (MSG_CNT_MAX + 1)

            next_at += interval
            sleep_for = next_at - time.perf_counter()
            if sleep_for > 0:
                time.sleep(sleep_for)
            else:
                # 发送慢于预期时不要累积滞后，直接对齐（真实压测里要单独统计"超频"）
                next_at = time.perf_counter()

    # ------------------------------------------------------------ 统计

    @property
    def sent_count(self) -> int:
        with self._lock:
            return len(self.sent)

    def sent_sequence(self):
        with self._lock:
            return [cnt for _, cnt in self.sent]
