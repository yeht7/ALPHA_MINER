# Quant Factor Mining Framework

基于 IBKR 数据源 + Polars 向量化引擎的因子挖掘框架。

## 项目结构

```
data_layer/          # Part 1 – 数据采集与缓存
  api.py             # get_stock_data() 统一入口
  cache_manager.py   # Parquet 本地缓存（去重 & 增量合并）
  fetcher.py         # IBKR 异步拉取
  ib_client.py       # IB 连接管理

alpha_miner/         # Part 2 – 因子计算引擎
  base.py            # BaseFactor 抽象基类
  feeder.py          # load_data() 从缓存加载长格式 DataFrame
  pipeline.py        # FactorPipeline 批量编排
  factors/
    trend.py         # IntradayVWAPDeviation（动量/趋势）
    microstructure.py# VolumePriceTrend（微观结构代理）

backtester/          # Part 3 – 向量化回测引擎
  aligner.py         # 信号-价格对齐 & 前瞻收益计算
  allocator.py       # 截面排名 & 分位数权重分配
  executor.py        # 模拟执行：换手率 & 交易成本
  metrics.py         # BacktestResult / 绩效指标
  engine.py          # VectorizedBacktester 编排器

evaluation/          # Part 4 – 绩效评估 & Tear Sheet
  data_prep.py       # 行业元数据拼接
  ic_stats.py        # IC / Rank IC 引擎
  bucket_stats.py    # 分位数桶收益 & 多空价差
  plotting.py        # EvaluationPlotter 绘图引擎
  tearsheet.py       # Tear Sheet 一键生成

execution/           # Part 5 – 模拟盘执行引擎
  state_manager.py   # PortfolioManager：持仓 & NLV 同步
  target_translator.py # 目标权重 → 股数差额计算
  risk_manager.py    # 风控拦截（仓位上限 / 杠杆 / 黑名单）
  router.py          # OrderRouter：MKT / MOC / LMT 下单
  tracker.py         # 异步成交追踪 & Parquet 日志
  main_job.py        # 完整 Rebalance 循环入口

data_cache/          # 自动生成 – OHLCV Parquet 缓存
signals/             # 自动生成 – 因子信号 Parquet 输出
trade_logs/          # 自动生成 – 成交日志（Parquet）

inspect_data.py      # 数据查询 CLI 工具
main.py              # 端到端运行示例（因子→回测→Tear Sheet）
```

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt   # 或 uv sync
```

### 2. 获取数据（需要 IBKR Gateway）

```python
import asyncio
from data_layer.api import get_stock_data

data = asyncio.run(get_stock_data(["AAPL", "MSFT"], timeframe="1 day"))
# 结果自动缓存到 ./data_cache/AAPL_1_day.parquet 等
```

### 3. 运行已有因子

```python
from alpha_miner.pipeline import FactorPipeline
from alpha_miner.factors.trend import IntradayVWAPDeviation
from alpha_miner.factors.microstructure import VolumePriceTrend

pipeline = FactorPipeline(
    factors=[IntradayVWAPDeviation(window=20), VolumePriceTrend(span=20)],
    tickers=["AAPL", "MSFT"],
)
results = pipeline.run()
# 信号自动保存到 ./signals/<FactorName>.parquet
```

### 4. 运行回测

```python
import polars as pl
from alpha_miner.pipeline import FactorPipeline
from alpha_miner.factors.trend import IntradayVWAPDeviation
from backtester import VectorizedBacktester

# 计算因子信号
pipeline = FactorPipeline(
    factors=[IntradayVWAPDeviation(window=20)],
    tickers=["AAPL", "MSFT"],
)
results = pipeline.run()
signals = results["IntradayVWAPDeviation"]

# 加载价格数据
from alpha_miner.feeder import load_data
prices = load_data(["AAPL", "MSFT"])

# 向量化回测
bt = VectorizedBacktester(
    delay=1,              # T 时刻信号 → T+1 执行
    quantiles=5,          # 五分位
    strategy="long_short",
    commission_rate=0.001,
    slippage_rate=0.0005,
)
result = bt.run(signals, prices)
print(result.summary())
# AnnReturn=+12.34%  AnnVol=15.67%  Sharpe=0.788  MaxDD=-8.23%  TotalTurnover=42.10

# 仅导出目标权重（可直接对接实盘系统）
weights = bt.generate_weights(signals, prices)
weights.write_parquet("target_weights.parquet")
```

### 5. 生成因子评估报告（Tear Sheet）

```python
from backtester.aligner import align_data
from alpha_miner.feeder import load_data
from evaluation.tearsheet import evaluate, create_full_tearsheet

# 准备对齐数据（信号 + 价格 → forward_return）
prices = load_data(["AAPL", "MSFT", "GOOGL", "AMZN", "JPM"])
aligned = align_data(signals, prices, delay=1)

# 运行全部统计计算
results = evaluate(aligned, quantiles=5)

# 打印 IC 摘要
print(results["ic_summary"])
# {'ic_mean': 0.0312, 'rank_ic_ir': 0.45, 'ic_positive_pct': 0.58, ...}

# 生成 Tear Sheet（PNG 或 PDF）
create_full_tearsheet(results, "output/tearsheet.png")

# 如果有回测结果，可以同时展示 Sharpe / MaxDD
from backtester import VectorizedBacktester
bt = VectorizedBacktester(delay=1, quantiles=5, strategy="long_short")
bt_result = bt.run(signals, prices)
create_full_tearsheet(results, "output/tearsheet.pdf", backtest_result=bt_result)
```

### 6. 查询本地数据

```bash
# 列出 data_cache 和 signals 中所有文件概览
python inspect_data.py ls

# 只看缓存 / 只看信号
python inspect_data.py ls cache
python inspect_data.py ls signals

# 查看某只股票（自动匹配 data_cache/AAPL_1_day.parquet）
python inspect_data.py show AAPL --head 5 --tail 5 --describe

# 查看某个信号文件
python inspect_data.py show ShortTermReversal --describe

# 只看统计摘要
python inspect_data.py stats AAPL
```

### 7. 运行模拟盘 Rebalance（Paper Trading）

> **前置条件：** IB Gateway (Paper Trading) 运行在 `localhost:4002`，且 `data_cache/` 中已有历史数据。

```python
import asyncio
from execution.main_job import run_rebalance_cycle

# Dry Run（默认）——只打印预期订单，不实际下单
asyncio.run(run_rebalance_cycle(dry_run=True))

# 真实下单到模拟盘
asyncio.run(run_rebalance_cycle(
    dry_run=False,
    order_type="MKT",              # 支持 "MKT" / "MOC" / "LMT"
    max_position_pct=0.05,         # 单笔交易 ≤ 5% NLV
    max_gross_leverage=1.0,        # 总杠杆上限
    restricted=["GME", "AMC"],     # 黑名单
))

# 定时循环（每日执行一次）
from execution.main_job import run_scheduled
asyncio.run(run_scheduled(interval_seconds=86400, dry_run=True))
```

```bash
# 命令行一键执行（dry-run）
python -m execution.main_job
```

## 如何新增自定义因子

只需三步：

**第一步** — 继承 `BaseFactor`，实现 `compute` 方法：

```python
# alpha_miner/factors/my_factor.py
import polars as pl
from alpha_miner.base import OUTPUT_SCHEMA, BaseFactor

class MyFactor(BaseFactor):
    def __init__(self, param: int = 10) -> None:
        self.param = param

    def compute(self, data: pl.DataFrame) -> pl.DataFrame:
        # data 包含列: datetime, ticker, open, high, low, close, volume
        # 必须返回且仅返回: datetime, ticker, factor_value
        return (
            data
            .with_columns(
                pl.col("close")
                  .pct_change()
                  .rolling_mean(window_size=self.param)
                  .over("ticker")
                  .alias("factor_value")
            )
            .select(OUTPUT_SCHEMA)
        )
```

**第二步** — 注册到 Pipeline 并运行：

```python
from alpha_miner.pipeline import FactorPipeline
from alpha_miner.factors.my_factor import MyFactor

pipeline = FactorPipeline(
    factors=[MyFactor(param=10)],
    tickers=["AAPL", "MSFT"],
)
pipeline.run()  # -> ./signals/MyFactor.parquet
```

**第三步** — 编写测试（重点检查前瞻偏差）：

```python
# tests/test_miner.py 中已有通用的前瞻偏差检测框架，直接复用：
def test_my_factor_no_lookahead(self):
    self._check_no_lookahead(MyFactor(param=5), _synthetic_data())
```

## 回测引擎设计

| 模块 | 职责 |
|------|------|
| `aligner` | 按 `delay` 参数将信号前移，与价格合并，计算前瞻收益 $r_{t+1} = (P_{t+1} - P_t) / P_t$ |
| `allocator` | 截面排名 → 分位数分桶 → 生成 `target_weight`（long\_short / long\_only） |
| `executor` | 组合总收益、漂移后换手率、交易成本 |
| `metrics` | 权益曲线、年化收益/波动、Sharpe、最大回撤 |
| `engine` | `VectorizedBacktester` 一键编排；`generate_weights()` 可独立导出到实盘 |

## 评估引擎设计

| 模块 | 职责 |
|------|------|
| `data_prep` | 将对齐数据与 GICS 行业分类元数据拼接，为行业中性化做准备 |
| `ic_stats` | 按日期分组计算 Pearson IC、Spearman Rank IC，汇总 IC IR / 正比例 |
| `bucket_stats` | 截面分位数桶收益；支持行业中性化模式；Top-Minus-Bottom 多空价差 |
| `plotting` | `EvaluationPlotter` — IC 时序图、桶累计收益线图、桶平均收益柱状图（原始 & Demean）、行业敞口堆积面积图 |
| `tearsheet` | `create_full_tearsheet()` — 用 `gridspec` 组装单页 Dashboard 并输出 PNG/PDF |

## 执行引擎设计

| 模块 | 职责 |
|------|------|
| `state_manager` | `PortfolioManager` — 从 IB Gateway 读取持仓快照、账户 NLV、实时行情价格 |
| `target_translator` | `calculate_order_delta()` — 目标权重 × NLV → 目标股数 → 与当前持仓做差 → `OrderDelta` 列表 |
| `risk_manager` | `RiskController` — 三重拦截：单票仓位超限 / 总杠杆超限 / 黑名单股票 |
| `router` | `OrderRouter` — 构造 `ib_async` 合约 + 订单对象，支持 MKT / MOC / LMT，`dry_run` 安全开关 |
| `tracker` | `TradeTracker` — 订阅 `orderStatusEvent` 异步追踪成交状态，flush 到 `trade_logs/` Parquet |
| `main_job` | `run_rebalance_cycle()` — 一键编排完整链路：Alpha → IB 连接 → 状态同步 → Delta → 风控 → 下单 → 成交追踪 |

### Rebalance 数据流

```
┌─────────────┐    ┌──────────────┐    ┌───────────────┐    ┌──────────────┐
│ Alpha Miner │───▶│   Delta Eng  │───▶│ Risk Manager  │───▶│ Order Router │
│  (weights)  │    │ (share delta)│    │  (validate)   │    │  (MKT/LMT)  │
└─────────────┘    └──────────────┘    └───────────────┘    └──────┬───────┘
       ▲                  ▲                                        │
       │                  │                                        ▼
┌──────┴──────┐    ┌──────┴──────┐                          ┌─────────────┐
│ Factor Pipe │    │  Portfolio  │                          │   Tracker   │
│  + Backtest │    │  Manager    │                          │ (fills log) │
│  Allocator  │    │ (IB state)  │                          └──────┬──────┘
└─────────────┘    └─────────────┘                                 │
                                                                   ▼
                                                            trade_logs/*.parquet
```

### 风控参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `max_position_pct` | 5% | 单笔交易金额 ≤ NLV × 5% |
| `max_gross_leverage` | 1.0 | Σ\|target_weight\| ≤ 1.0，超限时拒绝全部订单 |
| `restricted_list` | `[]` | 黑名单 ticker，命中则跳过该订单 |
| `dry_run` | `True` | **硬编码默认安全**——仅日志输出，不实际下单 |

### 订单类型

| 类型 | 说明 |
|------|------|
| `MKT` | Market Order，立即成交（默认） |
| `MOC` | Market-on-Close，收盘前集合竞价成交 |
| `LMT` | Limit Order，需传入 `limit_prices` 字典指定限价 |

## 关键约束

| 规则 | 说明 |
|------|------|
| **输出三列合约** | `compute()` 返回必须严格为 `['datetime', 'ticker', 'factor_value']` |
| **禁止 Python 行循环** | 所有计算必须用 Polars 表达式（`over("ticker")` 实现分组） |
| **无前瞻偏差** | T 时刻的因子值只能使用 t ≤ T 的数据，用 rolling / ewm 系列函数自动保证 |
| **可扩展到 C++/CUDA** | `compute()` 接口足够简单，内部可替换为 native kernel 调用 |
| **权重与 PnL 解耦** | `generate_weights()` 独立输出目标权重，可直接对接 Alpha Arena 等实盘平台 |
| **Dry Run 优先** | 执行引擎默认 `dry_run=True`，只记录日志不实际下单 |
| **成交日志持久化** | 每笔成交（价格、数量、时间、佣金）写入 `trade_logs/` Parquet，用于 Backtest vs Paper 滑点分析 |

## 测试

```bash
pytest tests/ -v
```
