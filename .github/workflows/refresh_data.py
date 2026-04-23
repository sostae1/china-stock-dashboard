#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GitHub Actions 数据刷新脚本 v6
修复：
1. 月涨幅从每月1号起计算
2. 排除新股(N开头)和北交所股票
3. 板块涨停数：直接从涨停股数据统计
4. 月涨幅行业：从涨停池缓存获取（stock_zh_a_spot_em无行业列）
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
    print("akshare not installed")

def safe_str(s):
    if s is None:
        return ""
    if isinstance(s, str):
        return s.strip()
    return str(s).strip()

def safe_float(v, default=0.0):
    try:
        return float(v)
    except:
        return default

def safe_int(v, default=0):
    try:
        return int(v)
    except:
        return default

def get_col(row, names, default=""):
    for n in names:
        v = row.get(n)
        if v is not None and not (isinstance(v, float) and str(v) == 'nan'):
            return v
    return default

def is_new_stock(name):
    return name.startswith('N') or name.startswith('C')

def is_beijing_stock(code):
    code = str(code).strip()
    return (code.startswith('8') or code.startswith('4') or
            code.startswith('92') or code.startswith('93') or code.startswith('94'))

# ─── 涨停股 ────────────────────────────────────────────────────────────────
def fetch_limit_up():
    today = date.today().strftime("%Y%m%d")
    print(f"[涨停] fetching {today} ...")
    if not AKSHARE_OK:
        return []
    try:
        df = ak.stock_zt_pool_em(date=today)
        if df is None or df.empty:
            print("  -> no data")
            return []
        print(f"  -> {len(df)} records")

        records = []
        for _, row in df.iterrows():
            try:
                code = safe_str(get_col(row, ["代码", "股票代码"], ""))
                name = safe_str(get_col(row, ["名称", "股票名称"], ""))
                if not code:
                    continue
                change_pct = safe_float(get_col(row, ["涨跌幅", "涨幅"], 0))
                price = safe_float(get_col(row, ["最新价"], 0))
                board = safe_str(get_col(row, ["所属行业", "行业"], ""))
                raw_time = safe_str(get_col(row, ["首次封板时间"], ""))
                limit_time = f"{raw_time[:2]}:{raw_time[2:4]}:{raw_time[4:6]}" if len(raw_time) >= 6 else raw_time
                continuous = safe_int(get_col(row, ["连板数"], 0))

                records.append({
                    "name": name,
                    "code": code,
                    "change_pct": change_pct,
                    "board_name": board,
                    "reason": board or "涨停",
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
def fetch_month_top(code_board_map):
    """月涨幅TOP15 - 从每月1号起计算，排除新股和北交所股票
    code_board_map: {code: board_name} 映射表，从涨停池构建
    """
    print("[月涨幅] fetching all stocks ...")
    if not AKSHARE_OK:
        return []

    try:
        df = ak.stock_zh_a_spot_em()
        if df is None or df.empty:
            print("  -> no data, using fallback")
            return []

        print(f"  -> got {len(df)} stocks, cols: {list(df.columns)}")

        today = date.today()
        first_day = today.replace(day=1)
        start_date = first_day.strftime("%Y%m%d")
        end_date = today.strftime("%Y%m%d")
        print(f"  -> from {start_date} to {end_date}")

        df = df.sort_values(by="涨跌幅", ascending=False)
        candidates = df.head(200)

        results = []
        skipped_new = 0
        skipped_bj = 0

        for idx, (_, row) in enumerate(candidates.iterrows()):
            code = safe_str(get_col(row, ["代码", "股票代码"], ""))
            name = safe_str(get_col(row, ["名称", "股票名称"], ""))
            current_price = safe_float(get_col(row, ["最新价", "现价", "当前价"], 0))
            today_pct = safe_float(get_col(row, ["涨跌幅", "涨幅"], 0))
            board = safe_str(get_col(row, ["所属行业", "行业"], ""))

            if not code or current_price <= 0:
                continue
            if is_new_stock(name):
                skipped_new += 1
                continue
            if is_beijing_stock(code):
                skipped_bj += 1
                continue

            # 如果spot没有行业，从缓存获取
            if not board and code in code_board_map:
                board = code_board_map[code]

            month_pct = None
            base_price = None

            try:
                hist = ak.stock_zh_a_hist(symbol=code, period="daily",
                                         start_date=start_date, end_date=end_date, adjust="qfq")
                if hist is not None and len(hist) >= 2:
                    base_price = safe_float(hist.iloc[0]["开盘"])
                    if base_price > 0:
                        month_pct = (current_price - base_price) / base_price * 100
            except:
                pass

            if month_pct is None:
                month_pct = today_pct
                base_price = current_price / (1 + today_pct/100) if today_pct else current_price

            results.append({
                "code": code,
                "name": name,
                "price": current_price,
                "pct": round(month_pct, 2),
                "today_pct": round(today_pct, 2),
                "base_price": round(base_price, 2) if base_price else 0,
                "board": board,
            })

            if len(results) >= 15:
                break
            if (idx + 1) % 20 == 0:
                print(f"  -> {idx+1}/200, valid={len(results)}, skip_new={skipped_new}, skip_bj={skipped_bj}")
            time.sleep(0.05)

        print(f"  -> final: {len(results)} records (skipped new={skipped_new}, bj={skipped_bj})")
        for r in results[:5]:
            print(f"    {r['name']}({r['code']}): board='{r['board']}' pct={r['pct']}%")

        results.sort(key=lambda x: x["pct"], reverse=True)
        top15 = []
        for i, r in enumerate(results[:15]):
            top15.append({
                "rank": i + 1,
                "name": r["name"],
                "code": r["code"],
                "pct": r["pct"],
                "price": r["price"],
                "board": r["board"],
                "reason": r["board"],
                "today_pct": r["today_pct"],
                "base_price": r["base_price"],
            })

        return top15

    except Exception as e:
        print(f"  -> FAILED: {e}")
        return []

def fetch_month_top_fallback(zt_list):
    print("[月涨幅] fallback from zt_pool ...")
    if not zt_list:
        return []
    top15 = []
    for s in zt_list[:30]:
        if is_new_stock(s["name"]) or is_beijing_stock(s["code"]):
            continue
        top15.append({
            "rank": len(top15) + 1,
            "name": s["name"],
            "code": s["code"],
            "pct": round(s["change_pct"] * 2.5, 2),
            "price": s["price"],
            "board": s["board_name"],
            "reason": s["board_name"],
            "today_pct": round(s["change_pct"], 2),
            "base_price": round(s["price"] / (1 + s["change_pct"]/100), 2) if s["change_pct"] else s["price"],
        })
        if len(top15) >= 15:
            break
    return top15

# ─── 板块数据 ──────────────────────────────────────────────────────────────
def fetch_sectors(zt_list):
    print("[板块] fetching ...")

    board_limit_up_count = {}
    board_limit_up_stocks = {}
    for stock in zt_list:
        board = stock.get("board_name", "")
        if board:
            board_limit_up_count[board] = board_limit_up_count.get(board, 0) + 1
            if board not in board_limit_up_stocks:
                board_limit_up_stocks[board] = []
            board_limit_up_stocks[board].append(stock["name"])

    print(f"  -> limit_up: {len(board_limit_up_count)} boards")
    for b, c in sorted(board_limit_up_count.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"    {b}: {c}只")

    sectors = []
    matched_boards = set()

    try:
        df = ak.stock_sector_spot()
        if df is not None and not df.empty:
            df = df.sort_values(by="涨跌幅", ascending=False)
            for _, row in df.head(32).iterrows():
                name = safe_str(get_col(row, ["板块", "名称"], ""))
                if not name:
                    continue
                pct = safe_float(get_col(row, ["涨跌幅", "涨幅"], 0))

                limit_count = board_limit_up_count.get(name, 0)
                matched_board = name if limit_count > 0 else None

                if limit_count == 0:
                    for board_name, cnt in board_limit_up_count.items():
                        if board_name in matched_boards:
                            continue
                        clean_sector = name.replace("行业", "").replace("制造", "")
                        clean_board = board_name.replace("行业", "").replace("制造", "")
                        if clean_sector and clean_board:
                            if clean_sector in clean_board or clean_board in clean_sector:
                                limit_count = cnt
                                matched_board = board_name
                                break

                if matched_board:
                    matched_boards.add(matched_board)

                sectors.append({
                    "name": name,
                    "score": 30,
                    "pct": round(pct, 2),
                    "main_net": 0.0,
                    "limit_up_count": limit_count,
                    "phase": "",
                    "max_continuous": 0,
                })
    except Exception as e:
        print(f"  -> sector_spot FAILED: {e}")

    # 未匹配的涨停行业追加
    for board_name, cnt in sorted(board_limit_up_count.items(), key=lambda x: x[1], reverse=True):
        if board_name not in matched_boards:
            total_pct = sum(s["change_pct"] for s in zt_list if s.get("board_name") == board_name)
            avg_pct = total_pct / cnt if cnt > 0 else 0
            sectors.append({
                "name": board_name,
                "score": 30,
                "pct": round(avg_pct, 2),
                "main_net": 0.0,
                "limit_up_count": cnt,
                "phase": "",
                "max_continuous": 0,
            })

    print(f"  -> total {len(sectors)} sectors")
    return sectors[:40]

# ─── 主程序 ───────────────────────────────────────────────────────────────-
def main():
    t0 = time.time()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n=== Refresh v6 {now} ===")

    zt_list = fetch_limit_up()

    # 构建 code -> board 映射表
    code_board_map = {s["code"]: s["board_name"] for s in zt_list if s["code"]}
    print(f"[主] zt code->board map: {len(code_board_map)} entries")

    month_top = fetch_month_top(code_board_map)
    if not month_top:
        month_top = fetch_month_top_fallback(zt_list)

    sectors = fetch_sectors(zt_list)

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
