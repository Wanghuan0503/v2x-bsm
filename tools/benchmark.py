# -*- coding: utf-8 -*-
"""QoS 0 / 1 / 2 对比压测。

用法::

    python tools/benchmark.py --obus 50 --hz 10 --seconds 10
    python tools/benchmark.py --obus 30 --hz 10 --seconds 5 --json qos_compare.json

【重要：怎么正确解读结果】
MQTT 跑在 TCP 之上，而 TCP 是可靠传输。本机回环没有任何链路丢包，
所以三种 QoS **都应该是 0 丢包** —— 这不是实验失败，这是正确结果。

QoS 等级保障的不是"TCP 丢包"，而是**会话层面的投递**：
断线重连、broker 重启、会话恢复（clean session = false）。
所以本工具额外统计 **CPU 时间**，用来体现 QoS 等级带来的协议开销差异
（QoS 1 多一次 PUBACK 往返，QoS 2 多两次，即四次握手）。

要验证 QoS 对可靠性的真正价值，需要注入"连接中断"场景，而不是跑正常压测。
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
    p = argparse.ArgumentParser(description="QoS 0/1/2 对比压测")
    p.add_argument("--obus", type=int, default=50)
    p.add_argument("--hz", type=float, default=10.0)
    p.add_argument("--seconds", type=float, default=10.0)
    p.add_argument("--qos", type=int, nargs="*", default=[0, 1, 2], help="要对比的 QoS 等级")
    p.add_argument("--broker", default=None, help="host:port；不填则内置启动")
    p.add_argument("--json", dest="json_out", default=None)
    p.add_argument("--verbose", action="store_true")
    return p.parse_args(argv)


def run_one(host, port, qos, args):
    """跑一轮指定 QoS 的压测，返回指标字典。"""
    cloud = CloudSubscriber(host, port, qos=qos, client_id=f"cloud-q{qos}").start()
    obus = []
    try:
        for i in range(args.obus):
            obus.append(
                ObuSimulator(
                    host,
                    port,
                    obu_id=f"Q{qos}-OBU-{i + 1:04d}",   # 每轮用独立 id，避免轮次之间串数据
                    hz=args.hz,
                    qos=qos,
                    msg_cnt_start=(i * 7) % 128,
                ).start()
            )

        t0 = time.perf_counter()
        cpu0 = time.process_time()
        time.sleep(args.seconds)
        elapsed = time.perf_counter() - t0

        # 先统一截止，再统一收尾（否则吞吐量会虚高，见 v2x/obu.py 注释）
        for obu in obus:
            obu.signal_stop()
        for obu in obus:
            obu.close()
        cpu = time.process_time() - cpu0
        sent = sum(o.sent_count for o in obus)

        time.sleep(0.5)          # 等在途报文
        report = cloud.report()
    finally:
        for obu in obus:
            obu.stop()
        cloud.stop()

    lat = report["latency_ms"]
    seq = report["sequence"]
    return {
        "qos": qos,
        "elapsed_s": round(elapsed, 3),
        "sent": sent,
        "received": report["received"],
        "undecodable": report["undecodable"],
        "invalid": report["invalid"],
        "loss": seq["loss"],
        "duplicate": seq["duplicate"],
        "out_of_order": seq["out_of_order"],
        "loss_rate_percent": round(seq["loss"] / sent * 100, 4) if sent else 0.0,
        "latency_p50": lat.get("p50"),
        "latency_p95": lat.get("p95"),
        "latency_p99": lat.get("p99"),
        "latency_max": lat.get("max"),
        "latency_mean": round(lat["mean"], 3) if lat.get("count") else None,
        "cpu_seconds": round(cpu, 3),
        "cpu_us_per_msg": round(cpu * 1e6 / report["received"], 2) if report["received"] else None,
        "msg_per_sec": round(report["received"] / elapsed, 1) if elapsed else None,
    }


def main(argv=None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO if args.verbose else logging.CRITICAL)

    try:
        import paho.mqtt.client  # noqa: F401
    except ImportError:
        print("缺少依赖：pip install -r requirements.txt")
        return 2

    broker = None
    if args.broker:
        host, _, port = args.broker.partition(":")
        host, port = host or "127.0.0.1", int(port or 1883)
    else:
        broker = InProcessBroker()
        try:
            broker.start()
        except Exception as exc:
            print(f"[broker] 内置 broker 启动失败：{exc}")
            return 3
        host, port = broker.addr

    print(f"配置：{args.obus} 路 OBU × {args.hz} Hz × {args.seconds}s，broker {host}:{port}")
    print()

    results = []
    try:
        for qos in args.qos:
            print(f"  正在跑 QoS {qos} ...", flush=True)
            results.append(run_one(host, port, qos, args))
    finally:
        if broker is not None:
            broker.stop()

    # ---------------- 对比表 ----------------
    rows = [
        ("发出条数", "sent", "{:d}"),
        ("接收条数", "received", "{:d}"),
        ("丢包 / 重复 / 乱序", None, None),
        ("丢包率 (%)", "loss_rate_percent", "{:.4f}"),
        ("端到端时延 P50 (ms)", "latency_p50", "{:.1f}"),
        ("端到端时延 P95 (ms)", "latency_p95", "{:.1f}"),
        ("端到端时延 P99 (ms)", "latency_p99", "{:.1f}"),
        ("端到端时延 MAX (ms)", "latency_max", "{:.1f}"),
        ("端到端时延 均值 (ms)", "latency_mean", "{:.3f}"),
        ("吞吐 (条/秒)", "msg_per_sec", "{:.1f}"),
        ("CPU 时间 (秒)", "cpu_seconds", "{:.3f}"),
        ("每条消息 CPU (us)", "cpu_us_per_msg", "{:.2f}"),
    ]

    width = 16
    header = "指标".ljust(width) + "".join(f"QoS {r['qos']}".rjust(14) for r in results)
    print()
    print("=" * len(header))
    print("            QoS 0 / 1 / 2 对比压测报告")
    print("=" * len(header))
    print(header)
    print("-" * len(header))
    for label, key, fmt in rows:
        if key is None:
            cells = "".join(
                f"{r['loss']} / {r['duplicate']} / {r['out_of_order']}".rjust(14) for r in results
            )
        else:
            cells = "".join(
                (fmt.format(r[key]) if r[key] is not None else "-").rjust(14) for r in results
            )
        print(label.ljust(width) + cells)
    print("=" * len(header))

    print()
    print("【结论提示】")
    print("  · 本机回环链路无丢包，三种 QoS 都不丢包是**正常且正确**的结果。")
    print("  · QoS 真正保障的是会话层投递（断线重连 / broker 重启 / 会话恢复），")
    print("    不是 TCP 丢包 —— 不要把结果解读成\"QoS 0 会丢包\"。")
    print("  · 可测的差异主要是协议开销：看 CPU 时间 / 每条消息 CPU 的上升趋势。")

    if args.json_out:
        Path(args.json_out).write_text(
            json.dumps(
                {
                    "config": {"obus": args.obus, "hz": args.hz, "seconds": args.seconds},
                    "results": results,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        print(f"\n结果已写入 {args.json_out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
