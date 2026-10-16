# -*- coding: utf-8 -*-
"""时延统计与消息序列分析 —— 纯 Python，零依赖。

刻意不引 numpy：一来减少依赖，二来面试时让你能手写百分位数。
"""

from __future__ import annotations

import math
from typing import Dict, Iterable, List, Sequence

DEFAULT_WRAP = 128  # 与 bsm.MSG_CNT_MAX + 1 一致


def percentile(values: Sequence[float], p: float) -> float:
    """最近秩（nearest-rank）百分位数。

    与 numpy 默认的线性插值法结果略有差异——测试报告里要注明用的是哪种，
    否则和同事对不上数。这里选最近秩是因为它总能返回真实存在的样本值。
    """
    if not 0 < p <= 100:
        raise ValueError(f"p 必须在 (0, 100] 内: {p}")
    data = sorted(values)
    if not data:
        raise ValueError("values 为空，无法计算百分位数")
    rank = math.ceil(p / 100.0 * len(data))
    return float(data[max(0, rank - 1)])


def summarize(values: Iterable[float]) -> Dict[str, float]:
    """时延等指标的汇总统计。"""
    data = [float(v) for v in values]
    if not data:
        return {"count": 0}
    return {
        "count": len(data),
        "min": min(data),
        "mean": sum(data) / len(data),
        "p50": percentile(data, 50),
        "p90": percentile(data, 90),
        "p95": percentile(data, 95),
        "p99": percentile(data, 99),
        "max": max(data),
    }


def analyze_sequence(seq: Sequence[int], wrap: int = DEFAULT_WRAP) -> Dict[str, int]:
    """按循环计数（msgCnt）分析丢包、重复与乱序。

    原理：BSM 的 msgCnt 是 0..127 循环递增的。
    接收端拿到的序列如果出现"向前跳 N"，就意味着中间丢了 N-1 条；
    出现"原地不动"是重复；出现"向后跳"是乱序。

    注意：这个方法的**前提是同一时刻只有一个发送端**。
    多 OBU 混在一起时必须先按 id 分组——见 cloud.CloudSubscriber.sequence_report。
    """
    if wrap <= 1:
        raise ValueError("wrap 必须大于 1")

    loss = duplicate = out_of_order = 0
    prev = None
    for cur in seq:
        if prev is not None:
            delta = (cur - prev) % wrap
            if delta == 0:
                duplicate += 1
            elif delta == 1:
                pass
            elif delta < wrap / 2:
                loss += delta - 1          # 向前跳 = 中间丢了
            else:
                out_of_order += 1          # 向后跳 = 乱序
        prev = cur

    return {
        "received": len(seq),
        "loss": loss,
        "duplicate": duplicate,
        "out_of_order": out_of_order,
    }


def latency_histogram(values: Sequence[float], buckets: int = 10):
    """给报告用的简易直方图（返回 (区间下界, 计数) 列表）。"""
    data = [float(v) for v in values]
    if not data:
        return []
    lo, hi = min(data), max(data)
    if hi == lo:
        return [(lo, len(data))]
    width = (hi - lo) / buckets
    bins: List[int] = [0] * buckets
    for v in data:
        idx = min(int((v - lo) / width), buckets - 1)
        bins[idx] += 1
    return [(lo + i * width, bins[i]) for i in range(buckets)]
