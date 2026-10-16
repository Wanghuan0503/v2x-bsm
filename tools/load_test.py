# -*- coding: utf-8 -*-
"""压测 / 报告入口：模拟 N 路 OBU 并发上报，输出一份可写进简历的测试报告。

用法::

    python tools/load_test.py --obus 1  --hz 10 --seconds 5      # 冒烟
    python tools/load_test.py --obus 50 --hz 10 --seconds 30     # 简历里那个数
    python tools/load_test.py --obus 20 --broker 127.0.0.1:1883  # 用外部 Mosquitto
    python tools/load_test.py --obus 50 --hz 10 --seconds 30 --json report.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from v2x.broker import InProcessBroker   # noqa: E402
from v2x.cloud import CloudSubscriber    # noqa: E402
from v2x.obu import ObuSimulator         # noqa: E402


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="V2X BSM 消息接入压测")
    p.add_argument("--obus", type=int, default=50, help="并发 OBU 数量")
    p.add_argument("--hz", type=float, default=10.0, help="每路上报频率")
    p.add_argument("--seconds", type=float, default=10.0, help="持续时间")
    p.add_argument("--qos", type=int, default=0, choices=[0, 1, 2])
    p.add_argument("--broker", default=None, help="host:port；不填则在本进程内启动 broker")
    p.add_argument("--json", dest="json_out", default=None, help="把报告写入 JSON 文件")
    p.add_argument("--verbose", action="store_true", help="打开 amqtt 内部日志（排查问题时用）")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)

    # amqtt 默认会往 root logger 打一堆 warning：sys_interval 未设置、
    # 客户端断开时的报文解码告警等。这些对使用者没有意义，默认静音。
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.CRITICAL,
        format="%(levelname)s %(name)s: %(message)s",
    )

    try:
        import paho.mqtt.client  # noqa: F401
    except ImportError:
        print("缺少依赖：pip install -r requirements.txt")
        return 2

    broker = None
    if args.broker:
        host, _, port = args.broker.partition(":")
        host, port = host or "127.0.0.1", int(port or 1883)
        print(f"[broker] 使用外部 broker {host}:{port}")
    else:
        broker = InProcessBroker()
        try:
            broker.start()
        except Exception as exc:
            print(f"[broker] 内置 broker 启动失败：{exc}")
            print("        可改用 --broker 127.0.0.1:1883 指向已安装的 Mosquitto")
            return 3
        host, port = broker.addr
        print(f"[broker] 已启动内置 broker {host}:{port}")

    cloud = None
    obus = []
    elapsed = 0.0
    try:
        cloud = CloudSubscriber(host, port, qos=args.qos).start()
        print(f"[cloud ] 订阅 {cloud.topic_filter}（QoS {args.qos}）")

        for i in range(args.obus):
            obus.append(
                ObuSimulator(
                    host,
                    port,
                    obu_id=f"OBU-{i + 1:04d}",
                    hz=args.hz,
                    qos=args.qos,
                    msg_cnt_start=(i * 7) % 128,   # 起始计数错开，更接近真实
                ).start()
            )
        print(f"[obu   ] {args.obus} 路已启动，每路 {args.hz} Hz，运行 {args.seconds}s ...")

        started_at = time.perf_counter()
        time.sleep(args.seconds)

        # 关键：先把所有 OBU 同时截止，再统一收尾。
        # 逐个 stop() 会让后面的 OBU 多跑一段（paho loop_stop 阻塞约 1s），
        # 统计时长不一致 → 吞吐量虚高。见 v2x/obu.py: signal_stop 的说明。
        elapsed = time.perf_counter() - started_at
        for obu in obus:
            obu.signal_stop()
        for obu in obus:
            obu.close()
        sent_total = sum(o.sent_count for o in obus)
        obus.clear()

        time.sleep(0.5)   # 留一点时间让在途报文到达
        report = cloud.report()
    finally:
        for obu in obus:
            obu.stop()
        if cloud is not None:
            cloud.stop()
        if broker is not None:
            broker.stop()

    # 用实测时长算理论值，而不是用参数里的 seconds
    expected = args.obus * args.hz * elapsed
    lat = report["latency_ms"]
    seq = report["sequence"]
    loss_rate = (seq["loss"] / expected * 100) if expected else 0.0

    print()
    print("=" * 46)
    print("        V2X BSM 消息接入测试报告")
    print("=" * 46)
    print(f"并发 OBU 数      : {args.obus}")
    print(f"上报频率 / QoS   : {args.hz} Hz / QoS {args.qos}")
    print(f"实测统计时长     : {elapsed:.2f} s")
    print(f"理论应到条数     : {expected:.0f}")
    print(f"实际发出条数     : {sent_total}")
    print(f"实际接收条数     : {report['received']}")
    print(f"解码失败         : {report['undecodable']}")
    print(f"字段校验失败     : {report['invalid']}")
    print(f"丢包 / 重复 / 乱序: {seq['loss']} / {seq['duplicate']} / {seq['out_of_order']}")
    print(f"丢包率           : {loss_rate:.4f} %")
    if lat.get("count"):
        print(
            "端到端时延(ms)   : "
            f"P50={lat['p50']:.1f}  P90={lat['p90']:.1f}  "
            f"P95={lat['p95']:.1f}  P99={lat['p99']:.1f}  MAX={lat['max']:.1f}"
        )
    else:
        print("端到端时延(ms)   : 无样本")
    print("=" * 46)

    if args.json_out:
        Path(args.json_out).write_text(
            json.dumps(
                {
                    "config": {
                        "obus": args.obus,
                        "hz": args.hz,
                        "seconds": args.seconds,
                        "qos": args.qos,
                    },
                    "elapsed_seconds": round(elapsed, 3),
                    "expected": round(expected, 1),
                    "sent_total": sent_total,
                    "report": report,
                    "loss_rate_percent": loss_rate,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        print(f"报告已写入 {args.json_out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
