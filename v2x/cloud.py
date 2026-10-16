# -*- coding: utf-8 -*-
"""云端订阅校验器：接收 BSM，做字段校验、时延统计、丢包/乱序分析。

对应真实场景里的"云"——数据接入层的第一道质量关卡。
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional

from . import metrics
from .bsm import MSG_CNT_MAX, decode, now_ms, validate_bsm
from .mqtt_compat import new_client

DEFAULT_TOPIC_FILTER = "v2x/obu/+/bsm"


class CloudSubscriber:
    def __init__(
        self,
        host: str,
        port: int,
        *,
        topic_filter: str = DEFAULT_TOPIC_FILTER,
        qos: int = 0,
        client_id: str = "cloud-validator",
    ) -> None:
        self.host = host
        self.port = port
        self.topic_filter = topic_filter
        self.qos = int(qos)
        self.client_id = client_id

        # records: 一条 = 一次成功收到的上报
        self.records: List[Dict[str, Any]] = []
        self.raw_undecodable: List[bytes] = []

        self._client = None
        self._lock = threading.Lock()
        self._connected = threading.Event()

    # ------------------------------------------------------------ 生命周期

    def start(self, wait_connected: float = 10.0) -> "CloudSubscriber":
        self._client = new_client(self.client_id)
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.connect(self.host, self.port, keepalive=30)
        self._client.loop_start()

        if not self._connected.wait(timeout=wait_connected):
            self._client.loop_stop()
            raise TimeoutError(f"云端订阅器在 {wait_connected}s 内未连上 {self.host}:{self.port}")
        return self

    def stop(self, timeout: float = 5.0) -> None:
        if self._client is not None:
            try:
                self._client.loop_stop()
                self._client.disconnect()
            except Exception:
                pass
            self._client = None

    def __enter__(self) -> "CloudSubscriber":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()

    # ------------------------------------------------------------ 回调

    def _on_connect(self, client, userdata, flags, *args):
        # 订阅放在 on_connect 里，保证"连接成功后才订阅"，避免启动竞态
        client.subscribe(self.topic_filter, qos=self.qos)
        self._connected.set()

    def _on_message(self, client, userdata, msg):
        # 收到就立刻打时间戳：晚一点打就会把处理耗时算进时延里
        recv_ts = now_ms()
        try:
            payload = decode(msg.payload)
        except Exception:
            with self._lock:
                self.raw_undecodable.append(bytes(msg.payload))
            return

        record = {
            "recvTs": recv_ts,
            "topic": msg.topic,
            "payload": payload,
            "obuId": payload.get("id") if isinstance(payload, dict) else None,
            "msgCnt": payload.get("msgCnt") if isinstance(payload, dict) else None,
            "sendTs": payload.get("sendTs") if isinstance(payload, dict) else None,
            "errors": validate_bsm(payload),
        }
        with self._lock:
            self.records.append(record)

    # ------------------------------------------------------------ 查询

    def wait_for(self, count: int, timeout: float = 5.0) -> bool:
        """等到累计收到 count 条（或超时）。返回是否达成。"""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.count >= count:
                return True
            time.sleep(0.02)
        return self.count >= count

    def reset(self) -> None:
        with self._lock:
            self.records.clear()
            self.raw_undecodable.clear()

    @property
    def count(self) -> int:
        with self._lock:
            return len(self.records)

    def snapshot(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self.records)

    # ------------------------------------------------------------ 分析

    def latencies_ms(self) -> List[float]:
        """应用层端到端时延（订阅端收到 - 发送端打点）。"""
        out = []
        for r in self.snapshot():
            if isinstance(r["sendTs"], (int, float)):
                out.append(float(r["recvTs"] - r["sendTs"]))
        return out

    def latency_summary(self) -> Dict[str, float]:
        return metrics.summarize(self.latencies_ms())

    def invalid_records(self) -> List[Dict[str, Any]]:
        return [r for r in self.snapshot() if r["errors"]]

    def by_obu(self) -> Dict[str, List[Dict[str, Any]]]:
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for r in self.snapshot():
            grouped[str(r["obuId"])].append(r)
        return dict(grouped)

    def sequence_report(self, obu_id: Optional[str] = None) -> Dict[str, int]:
        """丢包 / 重复 / 乱序分析。

        单 OBU 直接看它的 msgCnt 序列；
        多 OBU 混流时**必须先按 id 分组**，否则不同设备的计数会被当成乱序。
        """
        grouped = self.by_obu()
        if obu_id is not None:
            grouped = {obu_id: grouped.get(obu_id, [])}

        total = {"received": 0, "loss": 0, "duplicate": 0, "out_of_order": 0}
        for _id, recs in grouped.items():
            seq = [r["msgCnt"] for r in recs if isinstance(r["msgCnt"], int)]
            if not seq:
                continue
            report = metrics.analyze_sequence(seq, wrap=MSG_CNT_MAX + 1)
            for k in total:
                total[k] += report[k]
        return total

    def report(self) -> Dict[str, Any]:
        """汇总成一份可直接写进测试报告的结构。"""
        return {
            "received": self.count,
            "undecodable": len(self.raw_undecodable),
            "invalid": len(self.invalid_records()),
            "latency_ms": self.latency_summary(),
            "sequence": self.sequence_report(),
            "obu_count": len(self.by_obu()),
        }
