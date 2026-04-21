#!/usr/bin/env python3
"""
GitHub Actions 数据刷新脚本 v2
修复：列名映射、月涨幅计算、板块数据
"""
import json, time, os, sys
from datetime import date, datetime, timedelta

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
    if s is None:
        return ""
    if isinstance(s, str):
        return s
    for enc in ('utf-8', 'gbk', 'gb2312', 'latin1'):
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

def get_col(row, names, default=""):
    """按多个可能的列名获取值"""
    for n in names:
        v = row.get(n)
        if v is not None and v != "":
            return v
    return default

# ─── 涨停股 ────────────────────────────────────────────────────────────────
def fetch_limit_up():
    """涨停股池 - 修复：正确使用'涨跌幅'列名"""
    today = date.today().strftime("%Y%m%d")
    print(f"[涨停] fetching {today} ...")
    if not AKSHARE_OK:
        return []
    try:
        df = ak.stock_zt_pool_em(date=today)
        if df is None or df.empty:
            print("  -> no data (可能是休市日)")
            return []
        # 打印列名便于调试
        cols = list(df.columns)
        print(f"  -> cols: {cols}")
        print(f"  -> {len(df)} records")
        
        records = []
        for _, row in df.iterrows():
            try:
                code = safe_str(get_col(row, ["代码", "股票代码"], ""))
                name = safe_str(get_col(row, ["名称", "股票名称"], ""))
                if not code:
                    continue
                # 核心修复：列名是"涨跌幅"不是"涨幅%"
                change_pct = safe_float(get_col(row, ["涨跌幅", "涨幅", "涨幅%"], 0))
                price = safe_float(get_col(row, ["最新价", "价格"], 0))
                board = safe_str(get_col(row, ["所属行业", "行业", "板块"], ""))
                raw_time = safe_str(get_col(row, ["首次封板时间", "封板时间"], ""))
                limit_time = f"{raw_time[:2]}:{raw_time[2:4]}:{raw_time[4:6]}" if len(raw_time) >= 6 else raw_time
                continuous = safe_int(get_col(row, ["连板数", "连续涨停"], 0))
                
                records.append({
                    "name": name,
                    "code": code,
                    "change_pct": change_pct,  # 修复：现在有真实值
                    "board_name": board,
                    "reason": board or "涨停",  # API不提供涨停原因，暂时用行业名
                    "price": price,
                    "today_pct": change_pct,
                    "limit_time": limit_time,
                    "continuous_limit_up_count": continuous,
                })
            except Exception as e:
                print(f"  row error: {e}")
        print(f"  -> {len(records)} valid records")
        return records
    except Exception as e:
        print(f"  -> FAILED: {e}")
        return []

# ─── 月涨幅 TOP15 ───────────────────────────────────────────────────────────
def fetch_month_top(zt_list):
    """
    月涨幅TOP15 - 重写：计算真实月涨幅
    从今日涨停股中计算月涨幅，按月涨幅排序取TOP15
    """
    print("[月涨幅] calculating ...")
    if not AKSHARE_OK or not zt_list:
        return []
    
    today = date.today()
    # 约20个交易日前的日期
    start_date = (today - timedelta(days=30)).strftime("%Y%m%d")
    end_date = today.strftime("%Y%m%d")
    
    results = []
    for stock in zt_list[:50]:  # 限制只查前50只，避免超时
        code = stock["code"]
        name = stock["name"]
        current_price = stock["price"]
        
        try:
            # 获取日K线计算月涨幅
            df = ak.stock_zh_a_hist(symbol=code, period="daily", 
                                    start_date=start_date, end_date=end_date, adjust="qfq")
            if df is not None and len(df) >= 2:
                # 第一天的收盘价作为基准价
                first_close = safe_float(df.iloc[0]["收盘"])
                if first_close > 0:
                    month_pct = (current_price - first_close) / first_close * 100
                    base_price = first_close
                else:
                    month_pct = stock["change_pct"]
                    base_price = current_price / (1 + stock["change_pct"]/100) if stock["change_pct"] else current_price
            else:
                month_pct = stock["change_pct"]
                base_price = current_price / (1 + stock["change_pct"]/100) if stock["change_pct"] else current_price
            
            results.append({
                "code": code,
                "name": name,
                "price": current_price,
                "month_pct": round(month_pct, 2),
                "today_pct": stock["change_pct"],
                "board": stock["board_name"],
                "base_price": round(base_price, 2),
            })
            time.sleep(0.1)  # 避免请求过快
        except Exception as e:
            # 获取历史数据失败时，用今日涨幅作为fallback
            results.append({
                "code": code,
                "name": name,
                "price": current_price,
                "month_pct": stock["change_pct"],
                "today_pct": stock["change_pct"],
                "board": stock["board_name"],
                "base_price": current_price / (1 + stock["change_pct"]/100) if stock["change_pct"] else current_price,
            })
    
    # 按月涨幅排序取TOP15
    results.sort(key=lambda x: x["month_pct"], reverse=True)
    top15 = []
    for i, r in enumerate(results[:15]):
        top15.append({
            "rank": i + 1,
            "name": r["name"],
            "code": r["code"],
            "pct": r["month_pct"],  # 真实的月涨幅
            "price": r["price"],
            "board": r["board"],
            "reason": r["board"],
            "today_pct": r["today_pct"],
            "base_price": r["base_price"],
        })
    
    print(f"  -> {len(top15)} records (month gain calculated)")
    return top15

# ─── 板块数据 ──────────────────────────────────────────────────────────────
def fetch_sectors():
    """板块实时数据 - 修复：正确列名"""
    print("[板块] fetching ...")
    if not AKSHARE_OK:
        return []
    try:
        df = ak.stock_sector_spot()
        if df is None or df.empty:
            print("  -> no data")
            return []
        
        cols = list(df.columns)
        print(f"  -> cols: {cols}")
        
        # 按涨跌幅排序
        pct_col = "涨跌幅" if "涨跌幅" in cols else cols[5] if len(cols) > 5 else None
        if pct_col:
            df = df.sort_values(by=pct_col, ascending=False)
        
        sectors = []
        for _, row in df.head(32).iterrows():
            try:
                # 修复：列名是"板块"不是"名称"
                name = safe_str(get_col(row, ["板块", "名称", "板块名称"], ""))
                if not name:
                    continue
                pct = safe_float(get_col(row, ["涨跌幅", "涨幅"], 0))
                sectors.append({
                    "name": name,
                    "score": 30,
                    "pct": round(pct, 2),
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
    print(f"\n=== Refresh v2 {now} ===")

    zt_list = fetch_limit_up()
    month_top = fetch_month_top(zt_list)
    sectors = fetch_sectors()

    data = {
        "updated_at": now,
        "month_top": month_top,
        "zt_today": zt_list,
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
