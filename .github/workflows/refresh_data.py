#!/usr/bin/env python3
"""
GitHub Actions 数据刷新脚本
从 akshare 抓取数据，生成 snapshot_v5.json（UTF-8 编码）
"""
import json, time, os, sys, subprocess
from datetime import date, datetime

# 强制 stdout 为 utf-8
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

AKSHARE_OK = False
try:
    import akshare as ak
    AKSHARE_OK = True
    print(f"akshare {ak.__version__} loaded")
except ImportError:
    print("akshare not installed — will write placeholder data")

def safe_str(s):
    """将任意编码的字符串安全转为 UTF-8 str"""
    if s is None:
        return ""
    if isinstance(s, str):
        return s
    # 尝试解码 GBK（akshare 国内数据常用编码）
    for enc in ('gbk', 'gb2312', 'utf-8', 'latin1'):
        try:
            return s.decode(enc) if isinstance(s, bytes) else str(s)
        except (UnicodeDecodeError, AttributeError):
            pass
    return str(s)

def safe_float(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default

def safe_int(v, default=0):
    try:
        return int(v)
    except (TypeError, ValueError):
        return default

# ─── 涨停股 ────────────────────────────────────────────────────────────────
def fetch_limit_up():
    today = date.today().strftime("%Y%m%d")
    print(f"[涨停] fetching {today} ...")
    if not AKSHARE_OK:
        return []
    try:
        df = ak.stock_zt_pool_em(date=today)
        if df is None or df.empty:
            print(f"  -> no data")
            return []
        # 打印列名帮助调试
        print(f"  -> cols: {list(df.columns)[:8]}")
        records = []
        for _, row in df.iterrows():
            try:
                # 统一字段名（akshare 版本差异容错）
                name = safe_str(row.get("代码") or row.get("名称") or row.get("股票名称") or "")
                code = safe_str(row.get("代码") or row.get("股票代码") or "")
                if not code:
                    continue
                board = safe_str(
                    row.get("所属板块") or row.get("行业板块") or
                    row.get("板块") or row.get("概念板块") or ""
                )
                records.append({
                    "name": name,
                    "code": code,
                    "change_pct": safe_float(row.get("涨幅%", 0)),
                    "board_name": board,
                    "reason": board or "涨停",
                    "price": safe_float(row.get("最新价", 0)),
                    "today_pct": safe_float(row.get("涨幅%", 0)),
                    "limit_time": safe_str(row.get("首次封板时间", "")),
                    "continuous_limit_up_count": safe_int(row.get("连板数", 0)),
                })
            except Exception as e:
                pass
        print(f"  -> {len(records)} records")
        return records
    except Exception as e:
        print(f"  -> FAILED: {e}")
        return []

# ─── 月涨幅 TOP15 ─────────────────────────────────────────────────────────
def fetch_month_top():
    print("[月涨幅] fetching ...")
    if not AKSHARE_OK:
        return []
    try:
        df = ak.stock_spot_em()
        if df is None or df.empty:
            print("  -> no data")
            return []
        print(f"  -> cols: {list(df.columns)[:8]}")
        # 按涨跌幅排序取前15
        pct_col = "涨跌幅" if "涨跌幅" in df.columns else "涨跌比例"
        df = df.sort_values(by=pct_col, ascending=False)
        top = []
        for i, (_, row) in enumerate(df.head(15).iterrows()):
            try:
                code = safe_str(row.get("代码", ""))
                name = safe_str(row.get("名称", ""))
                if not code:
                    continue
                top.append({
                    "rank": i + 1,
                    "name": name,
                    "code": code,
                    "pct": safe_float(row.get(pct_col, 0)),
                    "price": safe_float(row.get("最新价", 0)),
                    "board": "",
                    "reason": "",
                    "today_pct": safe_float(row.get(pct_col, 0)),
                    "base_price": 0.0,
                })
            except Exception:
                pass
        print(f"  -> {len(top)} records")
        return top
    except Exception as e:
        print(f"  -> FAILED: {e}")
        return []

# ─── 板块数据 ──────────────────────────────────────────────────────────────
def fetch_sectors():
    print("[板块] fetching ...")
    if not AKSHARE_OK:
        return []
    try:
        df = ak.stock_sector_spot()
        if df is None or df.empty:
            print("  -> no data")
            return []
        print(f"  -> cols: {list(df.columns)[:8]}")
        pct_col = "涨跌幅" if "涨跌幅" in df.columns else "涨跌比例"
        df = df.sort_values(by=pct_col, ascending=False)
        sectors = []
        for _, row in df.head(32).iterrows():
            try:
                sectors.append({
                    "name": safe_str(row.get("板块名称", "")),
                    "score": 30,
                    "pct": safe_float(row.get(pct_col, 0)),
                    "main_net": 0.0,
                    "limit_up_count": 0,
                    "phase": "",
                    "max_continuous": 0,
                })
            except Exception:
                pass
        print(f"  -> {len(sectors)} sectors")
        return sectors
    except Exception as e:
        print(f"  -> FAILED: {e}")
        return []

# ─── 主程序 ────────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n=== Refresh {now} ===")

    zt_list   = fetch_limit_up()
    month_top = fetch_month_top()
    sectors   = fetch_sectors()

    data = {
        "updated_at": now,
        "month_top":  month_top,
        "zt_today":   zt_list,
        "sector_hot": sectors,
    }

    out = "snapshot_v5.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    size = os.path.getsize(out)
    print(f"\nWritten {size} bytes -> {out}")
    print(f"Done in {round(time.time()-t0,1)}s")
    return data

if __name__ == "__main__":
    main()
