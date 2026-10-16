# -*- coding: utf-8 -*-
"""端到端用例：OBU 上报 → broker → 云端订阅校验。

需要 paho-mqtt；没装时整个模块会被跳过（而不是报一堆 ImportError）。
"""

import uuid

import pytest

pytest.importorskip("paho.mqtt.client", reason="需要 paho-mqtt：pip install -r requirements.txt")

from v2x.cloud import CloudSubscriber   # noqa: E402
from v2x.obu import ObuSimulator        # noqa: E402


def _uid(prefix: str) -> str:
    """每次用例用独立 client_id，避免上一次的会话/遗嘱消息污染这一次。"""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def test_single_obu_reports_are_received_and_valid(broker_addr):
    host, port = broker_addr
    with CloudSubscriber(host, port, client_id=_uid("cloud")) as cloud:
        with ObuSimulator(host, port, obu_id=_uid("OBU"), hz=20.0) as obu:
            got = cloud.wait_for(10, timeout=5.0)
            obu.stop()
        report = cloud.report()

    assert got, f"5s 内只收到 {cloud.count} 条，低于预期"
    assert report["undecodable"] == 0, f"有 {report['undecodable']} 条无法解码"
    assert report["invalid"] == 0, f"有 {report['invalid']} 条字段校验失败"
    assert report["received"] >= 10
    # 本机回环 QoS0 实测丢包为 0；放宽到 1 是为了容忍 CI 抖动
    assert report["sequence"]["loss"] <= 1, f"丢包异常: {report['sequence']}"


def test_end_to_end_latency_within_threshold(broker_addr):
    host, port = broker_addr
    with CloudSubscriber(host, port, client_id=_uid("cloud")) as cloud:
        with ObuSimulator(host, port, obu_id=_uid("OBU"), hz=20.0) as obu:
            cloud.wait_for(20, timeout=5.0)
            obu.stop()
        stats = cloud.latency_summary()

    assert stats["count"] >= 20, f"样本不足: {stats}"
    # 本机回环通常在 20ms 以内；阈值放到 500ms 避免机器负载导致误报
    assert stats["p95"] < 500, f"P95 端到端时延过高: {stats}"
    assert stats["max"] < 2000, f"最大时延过高: {stats}"


def test_two_obus_are_grouped_separately(broker_addr):
    """两个 OBU 混流时，各自的 msgCnt 序列必须分开分析。

    如果直接把混流后的 msgCnt 丢进 analyze_sequence，会被全部判成"乱序"——
    这是新手最容易踩的坑，所以专门写一条用例把它钉住。
    """
    host, port = broker_addr
    id_a, id_b = _uid("OBU-A"), _uid("OBU-B")

    with CloudSubscriber(host, port, client_id=_uid("cloud")) as cloud:
        with ObuSimulator(host, port, obu_id=id_a, hz=10.0, msg_cnt_start=0) as obu_a:
            with ObuSimulator(host, port, obu_id=id_b, hz=10.0, msg_cnt_start=64) as obu_b:
                cloud.wait_for(20, timeout=5.0)
                obu_a.stop()
                obu_b.stop()
        report = cloud.report()
        grouped = cloud.by_obu()
        per_obu = {oid: cloud.sequence_report(obu_id=oid) for oid in grouped}

    assert len(grouped) == 2, f"应收到 2 个 OBU 的上报，实际 {list(grouped)}"
    for oid, recs in grouped.items():
        assert len(recs) >= 5, f"{oid} 只收到 {len(recs)} 条"
    # 分组后各自都不该出现乱序
    for oid, rep in per_obu.items():
        assert rep["out_of_order"] == 0, f"{oid} 分组后仍报乱序: {rep}"
    assert report["obu_count"] == 2


@pytest.mark.slow
def test_qos1_delivery_is_complete(broker_addr):
    """QoS1 下不应丢包——这是"QoS 等级对丢包影响"这条结论的实验依据。"""
    host, port = broker_addr
    with CloudSubscriber(host, port, client_id=_uid("cloud"), qos=1) as cloud:
        with ObuSimulator(host, port, obu_id=_uid("OBU"), hz=20.0, qos=1) as obu:
            cloud.wait_for(40, timeout=6.0)
            obu.stop()
        report = cloud.report()

    assert report["received"] >= 40, f"QoS1 下接收不足: {report['received']}"
    assert report["sequence"]["loss"] == 0, f"QoS1 下不应丢包: {report['sequence']}"
