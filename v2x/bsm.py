# -*- coding: utf-8 -*-
"""BSM（Basic Safety Message）报文的构造、编解码与字段校验。

【重要说明 —— 面试会被问到，务必看懂】
真实车路云系统中，BSM 属于国标 GB/T 31024 系列（CSAE 消息集）里的五类核心消息之一
（其余四类：SPaT、MAP、RSI、RSM），采用 ASN.1 UPER 编码为二进制后在 PC5 或 Uu 口传输。

本项目用 **JSON 等价建模**，目的是把"字段级测试"这件事跑通、可复现。
下面每个常量的取值范围都标了来源：

    【标准】参考 SAE J2735 / CSAE 消息集的常见定义
    【自定义】本项目自己约定的判定边界（真实项目中应由测试方案/需求方给定）

不要把【自定义】的边界当成标准去讲，否则面试官一追问就露馅。
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List

# ---------------------------------------------------------------- 取值边界

MSG_CNT_MAX = 127            # 【标准】J2735 msgCnt：0..127 循环计数
SEC_MARK_MAX = 59999         # 【标准】J2735 DSecond：分钟内的毫秒数 0..59999
LAT_MIN, LAT_MAX = -90.0, 90.0      # 【标准】纬度合法区间
LON_MIN, LON_MAX = -180.0, 180.0    # 【标准】经度合法区间
HEADING_MIN, HEADING_MAX = 0.0, 359.99   # 【标准】航向角
SPEED_MAX_MPS = 100.0        # 【自定义】本项目约定的车速上限（m/s）
ELEV_MIN, ELEV_MAX = -500.0, 8000.0  # 【自定义】海拔合理区间
ID_MAX_LEN = 32              # 【自定义】OBU 标识最大长度
ALLOWED_BRAKE_STATUS = {0, 1, 2, 3}  # 【自定义】制动系统状态枚举

REQUIRED_FIELDS = ("msgCnt", "id", "secMark", "pos", "speed", "heading", "sendTs")
POS_REQUIRED_FIELDS = ("lat", "lon")


def now_ms() -> int:
    """当前时间戳（毫秒）。测试时统一用它，项目里不要混用 time.time()。"""
    return int(time.time() * 1000)


# ---------------------------------------------------------------- 构造 / 编解码


def make_bsm(
    obu_id: str,
    msg_cnt: int,
    *,
    lat: float = 30.5231,
    lon: float = 114.3121,
    elev: float = 32.5,
    speed: float = 12.5,
    heading: float = 87.0,
    brake: int = 0,
    send_ts: int | None = None,
) -> Dict[str, Any]:
    """构造一条 BSM 报文（dict）。"""
    return {
        "msgCnt": int(msg_cnt),
        "id": obu_id,
        "secMark": now_ms() % 60000,
        "pos": {"lat": float(lat), "lon": float(lon), "elev": float(elev)},
        "speed": float(speed),
        "heading": float(heading),
        "brakeSysStatus": int(brake),
        "sendTs": now_ms() if send_ts is None else int(send_ts),
    }


def encode(payload: Dict[str, Any]) -> bytes:
    """序列化为 MQTT 载荷。紧凑分隔符可减小报文体积（真实场景下体积很敏感）。"""
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def decode(raw: bytes | str) -> Dict[str, Any]:
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    return json.loads(raw)


# ---------------------------------------------------------------- 校验


def _is_num(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def validate_bsm(payload: Any) -> List[str]:
    """校验一条 BSM。返回错误信息列表，空列表表示通过。

    刻意设计成"返回全部错误"而不是"抛第一个异常"——
    测试报告里需要知道一条报文到底违反了几条规则。
    """
    errors: List[str] = []

    if not isinstance(payload, dict):
        return ["报文不是 JSON 对象"]

    # 1. 必填字段存在性
    for field in REQUIRED_FIELDS:
        if field not in payload:
            errors.append(f"缺少必填字段: {field}")
    if errors:
        return errors  # 字段都缺了，后面的范围校验没有意义

    # 2. 标识
    obu_id = payload["id"]
    if not isinstance(obu_id, str) or not obu_id.strip():
        errors.append("id 必须是非空字符串")
    elif len(obu_id) > ID_MAX_LEN:
        errors.append(f"id 长度超过上限 {ID_MAX_LEN}: {len(obu_id)}")

    # 3. 循环计数与时间
    msg_cnt = payload["msgCnt"]
    if not isinstance(msg_cnt, int) or isinstance(msg_cnt, bool):
        errors.append("msgCnt 必须是整数")
    elif not 0 <= msg_cnt <= MSG_CNT_MAX:
        errors.append(f"msgCnt 越界(0..{MSG_CNT_MAX}): {msg_cnt}")

    sec_mark = payload["secMark"]
    if not isinstance(sec_mark, int) or isinstance(sec_mark, bool):
        errors.append("secMark 必须是整数")
    elif not 0 <= sec_mark <= SEC_MARK_MAX:
        errors.append(f"secMark 越界(0..{SEC_MARK_MAX}): {sec_mark}")

    if not _is_num(payload["sendTs"]):
        errors.append("sendTs 必须是数值")

    # 4. 位置
    pos = payload["pos"]
    if not isinstance(pos, dict):
        errors.append("pos 必须是对象")
    else:
        for f in POS_REQUIRED_FIELDS:
            if f not in pos:
                errors.append(f"pos 缺少字段: {f}")
        if "lat" in pos:
            if not _is_num(pos["lat"]):
                errors.append("pos.lat 必须是数值")
            elif not LAT_MIN <= pos["lat"] <= LAT_MAX:
                errors.append(f"pos.lat 越界({LAT_MIN}..{LAT_MAX}): {pos['lat']}")
        if "lon" in pos:
            if not _is_num(pos["lon"]):
                errors.append("pos.lon 必须是数值")
            elif not LON_MIN <= pos["lon"] <= LON_MAX:
                errors.append(f"pos.lon 越界({LON_MIN}..{LON_MAX}): {pos['lon']}")
        if "elev" in pos and _is_num(pos["elev"]):
            if not ELEV_MIN <= pos["elev"] <= ELEV_MAX:
                errors.append(f"pos.elev 越界({ELEV_MIN}..{ELEV_MAX}): {pos['elev']}")

    # 5. 动力学
    speed = payload["speed"]
    if not _is_num(speed):
        errors.append("speed 必须是数值")
    elif not 0 <= speed <= SPEED_MAX_MPS:
        errors.append(f"speed 越界(0..{SPEED_MAX_MPS}): {speed}")

    heading = payload["heading"]
    if not _is_num(heading):
        errors.append("heading 必须是数值")
    elif not HEADING_MIN <= heading <= HEADING_MAX:
        errors.append(f"heading 越界({HEADING_MIN}..{HEADING_MAX}): {heading}")

    # 6. 可选的制动状态（给了才校验）
    if "brakeSysStatus" in payload:
        brake = payload["brakeSysStatus"]
        if brake not in ALLOWED_BRAKE_STATUS:
            errors.append(f"brakeSysStatus 非法枚举值: {brake!r}")

    return errors


def is_valid(payload: Any) -> bool:
    return not validate_bsm(payload)
