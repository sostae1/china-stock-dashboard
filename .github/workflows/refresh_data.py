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

def decode_df(df):
    """将 DataFrame 列名和字符串内容从 GBK 解码为 UTF-8"""
    if df is None or df.empty:
        return df
    # 解码列名
    df.columns = [safe_str(c) for c in df.columns]
    # 解码所有字符串列
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(safe_str)
    return df

# ─── 涨停股 ────────────────────────────────────────────────────────────────
def fetch_limit_up():
    today = date.today().strftime("%Y%m%d")
    print(f"[涨停] fetching {today} ...")
    if not AKSHARE_OK:
        return []
    try:
        df = ak.stock_zt_pool_em(date=today)
        df = decode_df(df)
        if df is None or df.empty:
            print("  -> no data")
            return []
        print(f"  -> cols: {list(df.columns)}")
        print(f"  -> {len(df)} records")
        records = []
        for _, row in df.iterrows():
            try:
                code = safe_str(row.get("代码") or row.get("股票代码") or "")
                name = safe_str(row.get("名称") or row.get("股票名称") or "")
                if not code:
                    continue
                board = safe_str(row.get("所属行业") or row.get("行业") or row.get("所属板块") or "")
                # 格式化封板时间为 HH:MM:SS
                raw_time = safe_str(row.get("首次封板时间") or row.get("封板时间") or "")
                limit_time = f"{raw_time[:2]}:{raw_time[2:4]}:{raw_time[4:6]}" if len(raw_time) >= 6 else raw_time
                records.append({
                    "name": name,
                    "code": code,
                    "change_pct": safe_float(row.get("涨幅%", 0)),
                    "board_name": board,
                    "reason": board or "涨停",
                    "price": safe_float(row.get("最新价", 0)),
                    "today_pct": safe_float(row.get("涨幅%", 0)),
                    "limit_time": limit_time,
                    "continuous_limit_up_count": safe_int(row.get("连板数", 0)),
                })
            except Exception as e:
                print(f"  row error: {e}")
        print(f"  -> {len(records)} valid records")
        return records
    except Exception as e:
        print(f"  -> FAILED: {e}")
        return []

# ─── 月涨幅 TOP15（从涨停池中按涨幅排序）────────────────────────────────
def fetch_month_top():
    today = date.today().strftime("%Y%m%d")
    print(f"[月涨幅] fetching {today} ...")
    if not AKSHARE_OK:
        return []
    try:
        df = ak.stock_zt_pool_em(date=today)
        df = decode_df(df)
        if df is None or df.empty:
            print("  -> no data")
            return []
        # 按涨幅排序取前15
        pct_col = None
        for c in ["涨幅%", "涨幅", "change_pct", "涨跌幅"]:
            if c in df.columns:
                pct_col = c
                break
        if pct_col is None:
            print(f"  -> no pct col found, cols: {list(df.columns)}")
            return []
        df = df.sort_values(by=pct_col, ascending=False)
        top = []
        for i, (_, row) in enumerate(df.head(15).iterrows()):
            try:
                code = safe_str(row.get("代码") or row.get("股票代码") or "")
                name = safe_str(row.get("名称") or row.get("股票名称") or "")
                if not code:
                    continue
                pct = safe_float(row.get(pct_col, 0))
                top.append({
                    "rank": i + 1,
                    "name": name,
                    "code": code,
                    "pct": pct,
                    "price": safe_float(row.get("最新价", 0)),
                    "board": safe_str(row.get("所属行业") or ""),
                    "reason": safe_str(row.get("所属行业") or ""),
                    "today_pct": pct,
                    "base_price": 0.0,
                })
            except Exception as e:
                print(f"  row error: {e}")
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
        df = decode_df(df)
        if df is None or df.empty:
            print("  -> no data")
            return []
        pct_col = None
        for c in ["涨跌幅", "涨幅", "pct", "涨跌比例"]:
            if c in df.columns:
                pct_col = c
                break
        if pct_col is None:
            print(f"  -> no pct col, cols: {list(df.columns)}")
            return []
        df = df.sort_values(by=pct_col, ascending=False)
        sectors = []
        for _, row in df.head(32).iterrows():
            try:
                name = safe_str(row.get("名称") or row.get("板块名称") or row.get("label") or "")
                if not name:
                    continue
                sectors.append({
                    "name": name,
                    "score": 30,
                    "pct": safe_float(row.get(pct_col, 0)),
                    "main_net": 0.0,
                    "limit_up_count": 0,
                    "phase": "",
                    "max_continuous": 0,
                })
            except Exception as e:
                print(f"  row error: {e}")
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
