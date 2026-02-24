"""Interactive CLI to inspect data_cache and signals Parquet files."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl

CACHE_DIR = Path("./data_cache")
SIGNAL_DIR = Path("./signals")


# ------------------------------------------------------------------
# Listing
# ------------------------------------------------------------------

def _list_cache() -> None:
    files = sorted(CACHE_DIR.glob("*.parquet"))
    if not files:
        print("data_cache/ 为空")
        return
    print(f"\n{'─'*60}")
    print(f"  data_cache/  ({len(files)} 个文件)")
    print(f"{'─'*60}")
    print(f"  {'Ticker':<12} {'Rows':>8}  {'Date Range':<30} {'Size':>10}")
    print(f"  {'─'*12} {'─'*8}  {'─'*30} {'─'*10}")
    for f in files:
        df = pl.read_parquet(f)
        ticker = f.stem.rsplit("_1_day", 1)[0]
        n = len(df)
        size = f"{f.stat().st_size / 1024:.1f} KB"
        if "datetime" in df.columns and n > 0:
            dates = df["datetime"].sort()
            date_range = f"{str(dates[0])[:10]} → {str(dates[-1])[:10]}"
        else:
            date_range = "N/A"
        print(f"  {ticker:<12} {n:>8}  {date_range:<30} {size:>10}")


def _list_signals() -> None:
    files = sorted(SIGNAL_DIR.glob("*.parquet"))
    if not files:
        print("signals/ 为空")
        return
    print(f"\n{'─'*60}")
    print(f"  signals/  ({len(files)} 个文件)")
    print(f"{'─'*60}")
    print(f"  {'Factor':<28} {'Rows':>8}  {'Tickers':>8}  {'Date Range':<26}")
    print(f"  {'─'*28} {'─'*8}  {'─'*8}  {'─'*26}")
    for f in files:
        df = pl.read_parquet(f)
        name = f.stem
        n = len(df)
        n_tickers = df["ticker"].n_unique() if "ticker" in df.columns else 0
        if "datetime" in df.columns and n > 0:
            dates = df["datetime"].sort()
            date_range = f"{str(dates[0])[:10]} → {str(dates[-1])[:10]}"
        else:
            date_range = "N/A"
        print(f"  {name:<28} {n:>8}  {n_tickers:>8}  {date_range:<26}")


# ------------------------------------------------------------------
# Detail inspection
# ------------------------------------------------------------------

def _show_detail(path: Path, head: int, tail: int, describe: bool) -> None:
    if not path.exists():
        print(f"文件不存在: {path}")
        return
    df = pl.read_parquet(path)
    print(f"\n{'═'*60}")
    print(f"  {path}")
    print(f"  shape: {df.shape}  columns: {df.columns}")
    print(f"{'═'*60}")

    if describe:
        print("\n[describe]")
        print(df.describe())

    if head > 0:
        print(f"\n[head {head}]")
        print(df.head(head))

    if tail > 0:
        print(f"\n[tail {tail}]")
        print(df.tail(tail))


def _resolve_path(name: str) -> Path:
    """Try to find a parquet file from a short name like 'AAPL' or 'ShortTermReversal'."""
    p = Path(name)
    if p.exists():
        return p

    # data_cache: AAPL → data_cache/AAPL_1_day.parquet
    cache_path = CACHE_DIR / f"{name}_1_day.parquet"
    if cache_path.exists():
        return cache_path

    # signals: ShortTermReversal → signals/ShortTermReversal.parquet
    sig_path = SIGNAL_DIR / f"{name}.parquet"
    if sig_path.exists():
        return sig_path

    return p  # fallback, will report not found


# ------------------------------------------------------------------
# Factor query
# ------------------------------------------------------------------

def _query_signal(
    factor: str,
    ticker: str | None,
    date: str | None,
    top: int,
    sort_desc: bool,
) -> None:
    path = SIGNAL_DIR / f"{factor}.parquet"
    if not path.exists():
        print(f"信号文件不存在: {path}")
        return

    df = pl.read_parquet(path)

    if ticker:
        df = df.filter(pl.col("ticker") == ticker.upper())
    if date:
        prefix = date.replace(".", "-")
        df = df.filter(pl.col("datetime").cast(pl.Utf8).str.starts_with(prefix))

    df = df.sort("factor_value", descending=sort_desc)

    print(f"\n{'═'*60}")
    print(f"  Factor: {factor}  |  ticker={ticker or 'ALL'}  |  date={date or 'ALL'}")
    print(f"  匹配 {len(df)} 行 (显示 top {min(top, len(df))})")
    print(f"{'═'*60}\n")
    print(df.head(top))


# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="查询 data_cache & signals 中的 Parquet 数据",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command")

    # list
    ls = sub.add_parser("ls", help="列出所有文件概览")
    ls.add_argument("target", nargs="?", choices=["cache", "signals", "all"], default="all")

    # show
    show = sub.add_parser("show", help="查看某个文件的详细内容")
    show.add_argument("name", help="文件名/路径，如 AAPL、ShortTermReversal 或完整路径")
    show.add_argument("--head", type=int, default=5, help="显示前 N 行 (默认 5)")
    show.add_argument("--tail", type=int, default=5, help="显示后 N 行 (默认 5)")
    show.add_argument("--describe", action="store_true", help="输出 describe() 统计")

    # stats
    st = sub.add_parser("stats", help="对某个文件输出统计摘要")
    st.add_argument("name", help="文件名/路径")

    # query
    q = sub.add_parser(
        "query",
        help="查询因子信号值\n"
             "  例: query ShortTermReversal -t AAPL -d 2024-01\n"
             "      query ShortTermReversal -d 2024-01-08 --top 20",
    )
    q.add_argument("factor", help="因子名称，如 ShortTermReversal")
    q.add_argument("-t", "--ticker", help="股票代码，如 AAPL")
    q.add_argument("-d", "--date", help="日期前缀，如 2024-01-08 或 2024-01（支持 . 或 - 分隔）")
    q.add_argument("--top", type=int, default=20, help="最多显示行数 (默认 20)")
    q.add_argument("--asc", action="store_true", help="按 factor_value 升序排列（默认降序）")

    args = parser.parse_args()

    if args.command is None or args.command == "ls":
        target = getattr(args, "target", "all")
        if target in ("cache", "all"):
            _list_cache()
        if target in ("signals", "all"):
            _list_signals()

    elif args.command == "show":
        path = _resolve_path(args.name)
        _show_detail(path, args.head, args.tail, args.describe)

    elif args.command == "stats":
        path = _resolve_path(args.name)
        _show_detail(path, head=0, tail=0, describe=True)

    elif args.command == "query":
        _query_signal(args.factor, args.ticker, args.date, args.top, sort_desc=not args.asc)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
