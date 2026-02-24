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

execution/           # Part 5 – 执行引擎（模拟盘 & 实盘）
  state_manager.py   # PortfolioManager：持仓 & NLV 同步
  target_translator.py # 目标权重 → 股数差额计算
  risk_manager.py    # 风控拦截（仓位上限 / 杠杆 / 黑名单）
  router.py          # OrderRouter：MKT / MOC / LMT 下单
  tracker.py         # 异步成交追踪 & Parquet 日志
  main_job.py        # 完整 Rebalance 循环入口

data_cache/          # 自动生成 – OHLCV Parquet 缓存
signals/             # 自动生成 – 因子信号 Parquet 输出
trade_logs/          # 自动生成 – 成交日志（Parquet）
account_snapshots/   # 自动生成 – NLV & 持仓历史快照（Parquet）

inspect_data.py      # 数据查询 CLI 工具
inspect_account.py   # IB 账户 CLI（持仓/摘要/监控/绩效/滑点）
main.py              # 端到端运行示例（因子→回测→Tear Sheet）
test_live_order.py   # 实盘下单连通性测试（买卖 1 股往返）
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

### 6. 查询数据与因子信号

```bash
# 列出 data_cache 和 signals 中所有文件概览
python inspect_data.py ls

# 只看缓存 / 只看信号
python inspect_data.py ls cache
python inspect_data.py ls signals

# 查看某只股票原始数据
python inspect_data.py show AAPL --head 5 --tail 5 --describe

# 查看某个信号文件
python inspect_data.py show ShortTermReversal --describe

# 只看统计摘要
python inspect_data.py stats AAPL

# ── 因子信号查询 ──

# 查询 AAPL 在 2024 年 1 月的反转因子值
python inspect_data.py query ShortTermReversal -t AAPL -d 2024-01

# 查询 2024-01-08 当天所有股票的截面因子值（按因子值降序，取 top 10）
python inspect_data.py query ShortTermReversal -d 2024-01-08 --top 10

# 查询 AAPL 某天的因子值
python inspect_data.py query ShortTermReversal -t AAPL -d 2024-01-08

# 按因子值升序查看（找反转信号最弱的）
python inspect_data.py query ShortTermReversal -d 2024-01-08 --top 10 --asc
```

### 7. 查看 IB 账户状态

> 标注 **[离线]** 的命令无需 IB Gateway 在线。

```bash
# ── 持仓 ──
python inspect_account.py pos                        # 查看全部持仓
python inspect_account.py pos -t AAPL GOOG           # 只看指定股票
python inspect_account.py pos -s pnl_pct             # 按盈亏百分比排序（降序）
python inspect_account.py pos -s market_value --asc   # 按市值升序
python inspect_account.py pos --json                  # 额外输出 DataFrame 格式

# ── 账户摘要 ──
python inspect_account.py summary                    # 关键指标（NLV/现金/保证金等）
python inspect_account.py summary --all              # 显示全部账户字段

# ── 挂单 ──
python inspect_account.py orders                     # 查看当前活跃订单

# ── 成交日志 [离线] ──
python inspect_account.py logs --list                # 列出所有日志文件
python inspect_account.py logs                       # 查看最新一笔日志
python inspect_account.py logs -t SPY                # 按股票筛选
python inspect_account.py logs -f live_test          # 按文件名模糊匹配

# ── 账户监控 ──
python inspect_account.py monitor --once             # 采集一次快照后退出
python inspect_account.py monitor                    # 持续采集（默认 60 秒间隔）
python inspect_account.py monitor -i 300             # 每 5 分钟采集一次（Ctrl+C 停止）

# ── 绩效指标 [离线] ──
python inspect_account.py metrics                    # 全量账户 + 交易级指标
python inspect_account.py metrics -d 30              # 只看最近 30 天
python inspect_account.py metrics -t SPY             # 交易级指标只看 SPY

# ── 滑点分析 [离线] ──
python inspect_account.py slippage                   # 全部成交的滑点报告
python inspect_account.py slippage -t SPY            # 只看 SPY 的滑点
```

子命令均支持简写：`pos`/`p`、`summary`/`s`、`orders`/`o`、`logs`/`l`、`monitor`/`m`、`metrics`/`mt`、`slippage`/`sl`。

#### 监控 & 绩效工作流

```
1. 启动监控采集 NLV 快照（建议盘中每分钟/每小时跑一次）
   python inspect_account.py monitor -i 60

2. 积累数据后，查看绩效指标
   python inspect_account.py metrics

3. 每次 Rebalance 下单后，分析滑点
   python inspect_account.py slippage
```

#### 绩效指标说明

| 指标 | 来源 | 说明 |
|------|------|------|
| 年化收益率 | NLV 快照 | 日收益率均值 × 252 |
| 年化波动率 | NLV 快照 | 日收益率标准差 × √252 |
| Sharpe Ratio | NLV 快照 | 年化收益 / 年化波动 |
| 最大回撤 | NLV 快照 | 净值从峰值到谷值的最大跌幅 |
| Calmar Ratio | NLV 快照 | 年化收益 / 最大回撤 |
| 日胜率 | NLV 快照 | 正收益天数占比 |
| 盈亏比 | NLV 快照 | 平均盈利日收益 / 平均亏损日收益 |
| 期望值 (E) | NLV 快照 | 胜率 × 平均盈利 - 败率 × 平均亏损 |
| 交易胜率 | 成交日志 | FIFO 配对后盈利笔数占比 |
| 交易盈亏比 | 成交日志 | 平均盈利交易 / 平均亏损交易 |
| 滑点 (bps) | 成交日志 + 快照 | 成交价 vs 最近快照市价的偏差 |

### 8. 执行引擎：模拟盘 & 实盘

执行引擎（Part 5）同时支持 **模拟盘（Paper Trading）** 和 **实盘（Live Trading）**，通过 `.env` 中的 IB Gateway 连接参数区分目标账户。

#### 8.0 IB Gateway 连接配置

在项目根目录 `.env` 中配置连接参数：

```bash
# 模拟盘（Paper Trading）—— IB Gateway 默认端口
IB_GATEWAY_HOST=127.0.0.1
IB_GATEWAY_PORT=4002
IB_CLIENT_ID=1

# 实盘（Live Trading）—— 示例
# IB_GATEWAY_HOST=192.168.1.202
# IB_GATEWAY_PORT=4001
# IB_CLIENT_ID=1
```

| 环境变量 | 模拟盘默认值 | 说明 |
|----------|-------------|------|
| `IB_GATEWAY_HOST` | `127.0.0.1` | IB Gateway 所在机器 IP |
| `IB_GATEWAY_PORT` | `4002` | 4002 = Paper Trading，4001 = Live Trading |

| `IB_CLIENT_ID` | `1` | 多客户端并发时需不同 ID |

> **注意：** 模拟盘和实盘的唯一区别是 Gateway 端口。代码逻辑完全相同，`dry_run` 开关额外提供了安全保护。

#### 8.1 模拟盘 Rebalance（Paper Trading）

> **前置条件：** IB Gateway (Paper Trading) 运行中，`.env` 中 `IB_GATEWAY_PORT=4002`，且 `data_cache/` 中已有历史数据。

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

也可以显式指定连接参数（不依赖 `.env`）：

```python
asyncio.run(run_rebalance_cycle(
    ib_host="127.0.0.1",
    ib_port=4002,
    client_id=1,
    dry_run=False,
    order_type="MKT",
))
```

#### 8.2 实盘 Rebalance（Live Trading）

> **前置条件：** IB Gateway (Live Trading) 运行中，`.env` 中 `IB_GATEWAY_PORT` 指向实盘端口。

实盘与模拟盘使用同一套代码，只需切换 `.env` 中的端口：

```bash
# .env 切换为实盘
IB_GATEWAY_HOST=192.168.1.202
IB_GATEWAY_PORT=4001       # Live Trading 端口
IB_CLIENT_ID=1
```

```python
import asyncio
from execution.main_job import run_rebalance_cycle

# 建议先 dry-run 确认订单预期
asyncio.run(run_rebalance_cycle(dry_run=True))

# 确认无误后正式下单
asyncio.run(run_rebalance_cycle(
    dry_run=False,
    order_type="MKT",
    max_position_pct=0.05,
    max_gross_leverage=1.0,
    restricted=["GME", "AMC"],
))
```

或显式传入实盘参数（无需修改 `.env`）：

```python
asyncio.run(run_rebalance_cycle(
    ib_host="192.168.1.202",
    ib_port=4001,
    client_id=1,
    dry_run=False,
    order_type="MKT",
))
```

#### 8.3 实盘下单连通性测试

`test_live_order.py` 提供一键测试脚本：买入 1 股 SPY → 等待成交 → 立即卖出平仓，验证完整下单通道。

```bash
python test_live_order.py
```

测试流程：

1. 读取 `.env` 连接 IB Gateway
2. 获取账户 NLV 和当前持仓
3. 获取 SPY 延迟报价（无需实时行情订阅）
4. 通过 `OrderRouter` 提交 BUY 1 股 SPY 市价单
5. 通过 `TradeTracker` 等待成交确认
6. 立即提交 SELL 1 股 SPY 市价单平仓
7. 成交日志写入 `trade_logs/fills_live_test_*.parquet`

脚本执行前会要求确认（输入 `yes`），防止误操作。

#### 8.4 Rebalance 完整流程

无论模拟盘还是实盘，`run_rebalance_cycle()` 的执行步骤完全一致：

```
Alpha Pipeline    →  生成目标权重（target_weight）
        ↓
IB Gateway 连接   →  读取 NLV、当前持仓、实时价格
        ↓
Delta 计算        →  目标权重 × NLV ÷ 价格 = 目标股数，与当前持仓做差
        ↓
风控拦截          →  单票仓位 / 总杠杆 / 黑名单 三重检查
        ↓
下单路由          →  dry_run=True 仅日志，False 提交到 IB Gateway
        ↓
成交追踪          →  异步等待 Filled / Cancelled 状态
        ↓
日志持久化        →  trade_logs/*.parquet（含价格、数量、佣金、时间戳）
```

#### 8.5 安全机制

| 层级 | 机制 | 说明 |
|------|------|------|
| 代码默认 | `dry_run=True` | 不传参数时绝不实际下单 |
| 风控拦截 | `RiskController` | 单票仓位 > 5% NLV 或总杠杆 > 1.0 时拒绝订单 |
| 黑名单 | `restricted_list` | 指定 ticker 直接跳过 |
| 测试脚本 | 交互确认 | `test_live_order.py` 执行前需手动输入 `yes` |
| 网关隔离 | 端口区分 | Paper 4002 / Live 4001，物理隔离避免误操作 |

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
| **权重与 PnL 解耦** | `generate_weights()` 独立输出目标权重，可直接对接实盘系统 |
| **Dry Run 优先** | 执行引擎默认 `dry_run=True`，只记录日志不实际下单 |
| **模拟盘 / 实盘统一代码** | 同一套 Part 5 执行引擎，通过 `.env` 端口区分 Paper (4002) / Live (4001) |
| **成交日志持久化** | 每笔成交（价格、数量、时间、佣金）写入 `trade_logs/` Parquet，用于滑点分析 |

## 测试

```bash
pytest tests/ -v
```
