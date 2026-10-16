# V2X BSM 消息接入自动化测试

模拟 **OBU（车端）以固定频率上报 BSM 报文 → MQTT → 云控平台数据接入层**，
对消息接入做字段校验、端到端时延统计、丢包/重复/乱序分析，并输出可量化的测试报告。

对应车路云架构里的 **「云」这一层的数据接入质量关卡**，是「车路云测试」岗位最常做的工作之一。

---

## 1. 目录结构

```
v2x-bsm-test/
├── conftest.py                 # pytest 夹具：自动启动/销毁内置 broker
├── pytest.ini                  # 注意：本文件必须保持纯 ASCII（见"常见问题"）
├── requirements.txt
├── v2x/
│   ├── bsm.py                  # BSM 构造、编解码、字段校验规则
│   ├── metrics.py              # 百分位、汇总统计、序列分析（零依赖）
│   ├── broker.py               # 内置 amqtt broker（测试与压测共用）
│   ├── mqtt_compat.py          # paho-mqtt 1.x / 2.x 兼容层
│   ├── obu.py                  # OBU 模拟器（车端）
│   └── cloud.py                # 云端订阅校验器（云控平台）
├── tests/
│   ├── test_bsm_validation.py  # 字段校验用例（纯逻辑，无需 broker）
│   ├── test_metrics.py         # 时延与序列分析用例（纯逻辑）
│   └── test_e2e_bsm.py         # 端到端用例（需 paho-mqtt）
└── tools/
    └── load_test.py            # 并发压测 + 报告输出
```

---

## 2. 快速开始

```powershell
cd D:~v2x-bsm-test

# 1) 环境（amqtt 要求 Python >= 3.10）
conda create -n v2x python=3.11 -y
conda activate v2x
python -V                       # 确认 >= 3.10
pip install -r requirements.txt

# 2) 跑测试
pytest -q                       # 全量
pytest -q -m "not slow"         # 跳过慢用例
pytest --html=report.html       # 生成 HTML 报告（需 pytest-html）

# 3) 冒烟：1 路 OBU
python tools/load_test.py --obus 1 --hz 10 --seconds 5

# 4) 出简历里那个数：50 路并发
python tools/load_test.py --obus 50 --hz 10 --seconds 30 --json report_50obu.json
```

不需要你先手工启动任何 broker —— `conftest.py` 会自动在本进程内起一个 amqtt broker 并在测试结束后销毁。

---

## 3. 测试设计说明

### 3.1 用例分层（面试可以直接讲这个结构）

| 层 | 文件 | 特点 | 数量 |
|---|---|---|---|
| **字段校验层** | `test_bsm_validation.py` | 纯逻辑，毫秒级，可离线跑；覆盖正常/边界/异常 | 20+ |
| **统计逻辑层** | `test_metrics.py` | 纯逻辑；百分位、循环计数丢包判定 | 20 |
| **端到端层** | `test_e2e_bsm.py` | 真起 broker，验证"发得出、收得到、算得准" | 4 |

分层的好处：**核心判定逻辑不依赖网络和中间件**，CI 里跑得快、失败定位准；
端到端用例只负责验证接线。

### 3.2 关键判定准则（这部分是测试岗的核心能力，务必吃透）

| 指标 | 判定方法 | 陷阱 |
|---|---|---|
| **丢包** | 靠 `msgCnt`（0..127 循环计数）向前跳变检测：跳 N 表示丢 N-1 条 | 回绕（127→0）**不能**判成丢包 |
| **重复** | `msgCnt` 原地不动 | — |
| **乱序** | `msgCnt` 向后跳（回退超过半个循环） | — |
| **多 OBU** | **必须先按 id 分组**再分析序列 | 不分组会把不同设备的计数混在一起，全部误判成乱序 |
| **时延** | 订阅端收到时刻 − 发送端打点时刻（应用层端到端） | 时间戳要在收到消息的**第一行**打，晚了会把处理耗时算进去 |
| **百分位** | 最近秩法（返回值一定是真实样本） | 与 numpy 的线性插值法结果不同，报告里要注明用的哪种 |

### 3.3 BSM 字段取值范围

`v2x/bsm.py` 里每个常量都标了来源：

* **【标准】** 参考 SAE J2735 / CSAE 消息集常见定义（`msgCnt` 0..127、`secMark` 0..59999、纬度 ±90、经度 ±180、航向 0..359.99）
* **【自定义】** 本项目自己约定的边界（车速上限 100 m/s、id 长度 32、海拔区间、制动状态枚举）

> 另外：本项目用 JSON 等价建模。

---

## 4. 已验证状态与实测结果

### 4.1 验证状态

**本项目已完整跑通**，不是"写完没跑"的骨架。

| 部分 | 状态 |
|---|---|
| 字段校验逻辑（`bsm.py`） | ✅ 已验证 |
| 统计与序列分析（`metrics.py`） | ✅ 已验证（含回绕、重复、乱序、多 OBU 分组等边界） |
| paho-mqtt 2.x 兼容层 | ✅ 已验证（paho-mqtt 2.1.0 实测） |
| 内置 broker 启动 | ✅ 已验证（amqtt 0.12.0 实测） |
| MQTT 端到端全链路 | ✅ 已验证 |
| 并发压测工具 | ✅ 已验证 |

实测环境：Windows + Anaconda Python，`amqtt 0.12.0` + `paho-mqtt 2.1.0`。

```
$ pytest -q
44 passed
```

### 4.2 本机实测结果（50 路 OBU）

```
并发 OBU 数      : 50
上报频率 / QoS   : 10.0 Hz / QoS 0
实测统计时长     : 10.00 s
理论应到条数     : 5000
实际发出条数     : 5057
实际接收条数     : 5057
解码失败         : 0
字段校验失败     : 0
丢包 / 重复 / 乱序: 0 / 0 / 0
丢包率           : 0.0000 %
端到端时延(ms)   : P50=1.0  P90=1.0  P95=1.0  P99=1.0  MAX=4.0
```

完整数据见 `report_50obu.json`。

**关于 5057 vs 5000**：截止信号发出后，每个 OBU 还会跑完当前上报周期才退出，
所以"实际发出"会比理论值略高（50 路约多 1%）。
**判断丢包要看"实际发出 vs 实际接收"，以及 `msgCnt` 序列分析，不要拿理论值当分母。**

### 4.3 开发过程中踩到的两个坑（面试可以当谈资讲）

**坑 1：amqtt 的 `Broker` 必须在"运行中的"事件循环里构造**

`amqtt/broker.py` 里有一行：

```python
self._loop = loop or asyncio.get_running_loop()
```

注意是 `get_running_loop()` 而不是 `get_event_loop()` —— 光
`new_event_loop()` + `set_event_loop()` 不够，会直接抛
`RuntimeError: no running event loop`。
正确做法是在一个协程内部构造并启动 broker（见 `v2x/broker.py`）。

**坑 2：逐个 `stop()` 会让吞吐量统计虚高**

最早 `tools/load_test.py` 是这样写的：

```python
for obu in obus:
    obu.stop()      # 每停一个，paho 的 loop_stop() 阻塞近 1 秒
```

结果：轮不到停止的 OBU 还在继续上报，**index 越大的 OBU 统计时长越长**。
50 路跑 10 秒，理论 5000 条，实际统计出 17229 条（虚高 3.4 倍），
而丢包率还是 0 —— 自相矛盾。

定位方法：打印每路 OBU 的发出条数，看到
`[51, 61, 71, 81, ...]` 这种**随 index 递增**的规律，就锁定是停止顺序问题。

修复：拆成 `signal_stop()`（只置标志，立即返回）+ `close()`（统一收尾），
先让所有 OBU 同时截止。修复后每路发出条数完全一致，离散度为 0。

> 这个 bug 很有代表性：**测试工具本身出错，会让所有指标失真。**

---

## 5. 常见问题

### 5.1 内置 broker 起不来（amqtt 配置差异）

报错会形如：`内置 broker 启动失败（已尝试 3 套配置）：配置#0 启动失败: ...`

按顺序试：

1. **换成外部 broker（最稳，推荐）**
   ```powershell
   # 装 Mosquitto 的 Windows 安装包，启动服务后：
   $env:V2X_BROKER = "127.0.0.1:1883"
   pytest -q
   python tools/load_test.py --obus 50 --broker 127.0.0.1:1883
   ```
   用了 `V2X_BROKER` 后就不会再尝试内置 broker，代码一行都不用改。

2. **看 amqtt 版本对应文档**
   ```powershell
   pip show amqtt
   ```
   在 `v2x/broker.py` 的 `candidate_configs()` 里按你的版本补一套配置即可。

### 5.2 `UnicodeDecodeError: 'gbk' codec can't decode ...`

**这是中文版 Windows 的经典坑**：pytest 读取 `pytest.ini` 时用的是系统默认编码（GBK），不是 UTF-8。

**所以 `pytest.ini` 必须保持纯 ASCII**，不要在配置文件里写中文注释
（`.py` 文件不受影响，Python 源码默认按 UTF-8 解析）。

### 5.3 `paho-mqtt` 相关报错

paho-mqtt 2.x 改了回调 API：不再允许 `mqtt.Client()` 空参调用，必须传 `CallbackAPIVersion`。
本项目已用 `v2x/mqtt_compat.py` 统一兼容 1.x / 2.x，并在 2.1.0 上实测通过。

如果你自己写脚本遇到 `ValueError: Unsupported callback API version`，就是这个原因。

### 5.4 时延数字偏大

本机回环（loopback）通常在 20ms 以内。如果你的数字很大，依次排查：

* 时间戳是不是在回调函数**第一行**打的（晚打会把处理耗时算进去）；
* 是不是跑在虚拟机的 NAT 网络而不是本机回环；
* 断开调试器 —— 断点会让时延失去意义。

---

## 6. 怎么扩展（把简历数字做扎实）

按性价比排序：

1. **QoS 对比实验**：`--qos 0` / `--qos 1` / `--qos 2` 各跑一遍，比较丢包率与时延差异。
   这是"我验证过 QoS 等级对可靠性的影响"这句话的**唯一证据来源**。
2. **并发梯度压测**：`--obus` 从 1 / 10 / 50 / 100 各跑一轮，画出"并发数—P95 时延"曲线。
   这能让你讲出"broker 在多少并发后出现瓶颈"。
3. **异常注入用例**：在 `tests/` 里补 —— 非法坐标、缺字段、超频上报（100Hz）、中途断连重连。
   （`validate_bsm()` 已经支持，只需要构造坏报文发出去。）
4. **数据落地校验**：如果云侧有数据库，补一条"上报消息最终落库条数 == 收到条数"的用例。

---
## 8. 参考

* BSM / SPaT / MAP / RSI / RSM 为车路云五类核心消息（GB/T 31024 系列 / CSAE 消息集）
* [paho-mqtt 文档](https://eclipse.dev/paho/index.php?page=clients/python/docs/index.php)
* [amqtt 文档](https://amqtt.readthedocs.io/)
