# -*- coding: utf-8 -*-
"""BSM 字段校验用例 —— 纯逻辑，不依赖 MQTT，任何环境都能跑。"""

import copy

import pytest

from v2x.bsm import (
    MSG_CNT_MAX,
    decode,
    encode,
    is_valid,
    make_bsm,
    validate_bsm,
)


@pytest.fixture
def valid_bsm():
    return make_bsm("OBU-0001", 0, send_ts=1726470000123)


# ------------------------------------------------------------ 正常路径


def test_valid_bsm_passes(valid_bsm):
    assert validate_bsm(valid_bsm) == []
    assert is_valid(valid_bsm)


def test_encode_decode_roundtrip(valid_bsm):
    assert decode(encode(valid_bsm)) == valid_bsm


def test_msg_cnt_boundary_values():
    """边界值：0 与 127 都应通过，128 应被拒绝。"""
    assert is_valid(make_bsm("OBU-1", 0))
    assert is_valid(make_bsm("OBU-1", MSG_CNT_MAX))
    assert not is_valid(make_bsm("OBU-1", MSG_CNT_MAX + 1))


def test_speed_boundaries():
    assert is_valid(make_bsm("OBU-1", 1, speed=0.0))          # 静止
    assert is_valid(make_bsm("OBU-1", 1, speed=100.0))        # 上边界
    assert not is_valid(make_bsm("OBU-1", 1, speed=100.1))    # 越界
    assert not is_valid(make_bsm("OBU-1", 1, speed=-1.0))     # 负速度


def test_heading_boundaries():
    assert is_valid(make_bsm("OBU-1", 1, heading=0.0))
    assert is_valid(make_bsm("OBU-1", 1, heading=359.99))
    assert not is_valid(make_bsm("OBU-1", 1, heading=360.0))


# ------------------------------------------------------------ 异常路径


@pytest.mark.parametrize("missing", ["msgCnt", "id", "secMark", "pos", "speed", "heading", "sendTs"])
def test_missing_required_field_detected(valid_bsm, missing):
    payload = copy.deepcopy(valid_bsm)
    payload.pop(missing)
    errors = validate_bsm(payload)
    assert any(missing in e for e in errors), f"未检出缺失字段 {missing}: {errors}"


def test_lat_out_of_range(valid_bsm):
    payload = copy.deepcopy(valid_bsm)
    payload["pos"]["lat"] = 91.0
    assert any("pos.lat" in e for e in validate_bsm(payload))

    payload["pos"]["lat"] = -90.0      # 边界内，应通过
    assert validate_bsm(payload) == []


def test_lon_out_of_range(valid_bsm):
    payload = copy.deepcopy(valid_bsm)
    payload["pos"]["lon"] = -180.5
    assert any("pos.lon" in e for e in validate_bsm(payload))


def test_pos_missing_subfield(valid_bsm):
    payload = copy.deepcopy(valid_bsm)
    payload["pos"].pop("lat")
    assert any("pos 缺少字段: lat" in e for e in validate_bsm(payload))


def test_invalid_types(valid_bsm):
    payload = copy.deepcopy(valid_bsm)
    payload["msgCnt"] = "10"           # 字符串不算整数
    payload["speed"] = None
    errors = validate_bsm(payload)
    assert any("msgCnt" in e for e in errors)
    assert any("speed" in e for e in errors)


def test_bool_is_not_a_number(valid_bsm):
    """Python 里 bool 是 int 的子类，这是个经典坑，必须单独设防。"""
    payload = copy.deepcopy(valid_bsm)
    payload["msgCnt"] = True
    assert any("msgCnt" in e for e in validate_bsm(payload))


def test_bad_brake_status_enum(valid_bsm):
    payload = copy.deepcopy(valid_bsm)
    payload["brakeSysStatus"] = 99
    assert any("brakeSysStatus" in e for e in validate_bsm(payload))


def test_oversized_id(valid_bsm):
    payload = copy.deepcopy(valid_bsm)
    payload["id"] = "X" * 33
    assert any("id 长度" in e for e in validate_bsm(payload))


def test_non_dict_payload():
    assert validate_bsm([1, 2, 3]) == ["报文不是 JSON 对象"]


def test_multiple_errors_reported_together(valid_bsm):
    """一条报文违反多条规则时，应该全部报出来（便于定位），而不是只报第一条。"""
    payload = copy.deepcopy(valid_bsm)
    payload["speed"] = 999.0
    payload["heading"] = -5.0
    payload["pos"]["lat"] = 200.0
    errors = validate_bsm(payload)
    assert len(errors) >= 3
