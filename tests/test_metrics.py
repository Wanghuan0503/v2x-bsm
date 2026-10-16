# -*- coding: utf-8 -*-
"""时延统计与序列分析用例 —— 纯逻辑，不依赖 MQTT。"""

import pytest

from v2x.metrics import analyze_sequence, latency_histogram, percentile, summarize


# ------------------------------------------------------------ percentile


def test_percentile_nearest_rank():
    data = list(range(1, 101))          # 1..100
    assert percentile(data, 50) == 50
    assert percentile(data, 95) == 95
    assert percentile(data, 100) == 100
    assert percentile(data, 1) == 1


def test_percentile_ignores_input_order():
    assert percentile([5, 1, 3, 2, 4], 50) == 3


def test_percentile_returns_actual_sample():
    """最近秩法的特点：返回值一定是样本里真实存在的值。"""
    data = [1.0, 2.0, 3.0]
    assert percentile(data, 90) in data


def test_percentile_rejects_bad_input():
    with pytest.raises(ValueError):
        percentile([1, 2, 3], 0)
    with pytest.raises(ValueError):
        percentile([1, 2, 3], 101)
    with pytest.raises(ValueError):
        percentile([], 95)


# ------------------------------------------------------------ summarize


def test_summarize_basic_fields():
    stats = summarize([10, 20, 30, 40, 50])
    assert stats["count"] == 5
    assert stats["min"] == 10
    assert stats["max"] == 50
    assert stats["mean"] == 30
    assert stats["p50"] == 30


def test_summarize_empty():
    assert summarize([]) == {"count": 0}


def test_summarize_accepts_generator():
    assert summarize(x for x in [1, 2, 3])["count"] == 3


def test_summarize_p95_ge_p50():
    stats = summarize(range(1, 201))
    assert stats["p95"] >= stats["p50"] >= stats["min"]


# ------------------------------------------------------------ analyze_sequence


def test_perfect_sequence_has_no_problem():
    report = analyze_sequence(list(range(10)))
    assert report == {"received": 10, "loss": 0, "duplicate": 0, "out_of_order": 0}


def test_gap_counts_as_loss():
    # 0,1,2 → 5,6：跳过 3 和 4，丢 2 条
    report = analyze_sequence([0, 1, 2, 5, 6])
    assert report["loss"] == 2
    assert report["duplicate"] == 0


def test_wraparound_is_not_loss():
    """126,127,0,1 —— 循环计数回绕，绝不能判成丢包。"""
    report = analyze_sequence([126, 127, 0, 1])
    assert report["loss"] == 0
    assert report["out_of_order"] == 0


def test_duplicate_detected():
    report = analyze_sequence([0, 1, 1, 2])
    assert report["duplicate"] == 1
    assert report["loss"] == 0


def test_out_of_order_detected():
    report = analyze_sequence([0, 1, 5, 4, 6])
    assert report["out_of_order"] == 1
    assert report["duplicate"] == 0
    assert report["loss"] == 4          # 1→5 丢 3 条，5→4 乱序，4→6 丢 1 条


def test_empty_sequence():
    assert analyze_sequence([])["received"] == 0


def test_single_element_sequence():
    assert analyze_sequence([7]) == {"received": 1, "loss": 0, "duplicate": 0, "out_of_order": 0}


def test_invalid_wrap():
    with pytest.raises(ValueError):
        analyze_sequence([1, 2], wrap=1)


# ------------------------------------------------------------ histogram


def test_histogram_bucket_count_matches_samples():
    hist = latency_histogram([float(x) for x in range(100)], buckets=10)
    assert len(hist) == 10
    assert sum(count for _, count in hist) == 100


def test_histogram_single_value():
    assert latency_histogram([5.0, 5.0]) == [(5.0, 2)]


def test_histogram_empty():
    assert latency_histogram([]) == []
