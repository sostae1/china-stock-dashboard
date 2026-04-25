#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GitHub Actions 数据刷新脚本 v10b
月涨幅计算：Sina财经日K线（原始价格）+ 重试机制
- 找4月份第一个交易日开盘价
- 月涨幅 = (现价 - 4月首日开盘) / 4月首日开盘 × 100
- 候选池：涨停池 + 腾讯批量补充（覆盖月牛股）
"""
import json, time, os, sys, urllib.request
from datetime import date, datetime

import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

AKSHARE_OK = False
try:
    import akshare as ak
    AKSHARE_OK = True
    print(f"akshare {ak.__version__} loaded")
except ImportError:
    print("akshare not installed")

# ─── 工具函数 ────────────────────────────────────────────────────────────────
def safe_float(v, default=0.0):
    try:
        f = float(v)
        if str(f) in ('nan', 'inf'):
            return default
        return f
    except:
        return default

def safe_str(s):
    if s is None:
        return ""
    if isinstance(s, str):
        return s.strip()
    return str(s).strip()

def is_new_stock(name):
    n = safe_str(name)
    return n.startswith('N') or n.startswith('C')

def is_beijing_stock(code):
    c = safe_str(code)
    return c.startswith('8') or c.startswith('4') or c.startswith('92') or c.startswith('93') or c.startswith('94')

# ─── Sina财经日K线 ────────────────────────────────────────────────────────────
def fetch_sina_kline(code, count=60):
    """
    获取新浪财经日K（原始价格，无需复权）
    返回: [(date_str, open, close), ...]  升序
    带重试（网络不稳定时偶发失败）
    """
    c = str(code)
    prefix = "sz" if not c.startswith("6") else "sh"
    sym = f"{prefix}{c}"
    url = (f"https://money.finance.sina.com.cn/quotes_service/api/json_v2.php"
           f"/CN_MarketData.getKLineData?symbol={sym}&scale=240&ma=no&datalen={count}")
    
    last_err = None
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=12) as resp:
                raw = resp.read().decode("utf-8")
            if not raw or raw.strip() == "":
                last_err = "empty"
                time.sleep(0.5)
                continue
            data = json.loads(raw)
            if isinstance(data, list) and len(data) > 0:
                result = []
                for bar in data:
                    d = safe_str(bar.get("day", ""))
                    o = safe_float(bar.get("open", 0))
                    c2 = safe_float(bar.get("close", 0))
                    if d and o > 0:
                        result.append((d, o, c2))
                return result
            else:
                last_err = f"not-list or empty: {type(data)}"
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            if attempt < 2:
                time.sleep(0.5)
    
    return []  # 重试3次仍失败

def calc_month_pct(code, current_price):
    """
    用Sina财经K线计算月涨幅（4月基准）
    返回: (month_pct, base_price, first_april_date) 或 (None, None, None)
    """
    klines = fetch_sina_kline(code, 60)
    if not klines:
        return None, None, None
    
    # 找4月份第一个交易日
    first_april = None
    for bar in klines:
        d, _, _ = bar
        if "2026-04" in d or d.startswith("2026/04"):
            first_april = bar
            break
    
    if first_april is None:
        return None, None, None
    
    first_date, first_open, first_close = first_april
    base_price = first_open if first_open > 0 else first_close
    if base_price <= 0:
        return None, None, None
    
    month_pct = round((current_price - base_price) / base_price * 100, 2)
    return month_pct, base_price, first_date

# ─── 腾讯实时行情批量查询 ────────────────────────────────────────────────────
def fetch_tengxun_realtime(codes):
    """腾讯实时行情批量查询，返回 {symbol: {price, change_pct, name, sector}}"""
    if not codes:
        return {}
    results = {}
    for i in range(0, len(codes), 50):
        batch = codes[i:i+50]
        joined = ",".join(batch)
        url = f"https://qt.gtimg.cn/q={joined}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                raw = resp.read().decode("gbk", errors="replace")
            for line in raw.split(";"):
                eq = line.find("=")
                if eq < 0:
                    continue
                sym = line[:eq].replace("v_", "").strip()
                parts = line[eq+1:].strip().strip('"').split("~")
                if len(parts) < 35:
                    continue
                try:
                    results[sym] = {
                        "price": safe_float(parts[3]),
                        "change_pct": safe_float(parts[32]),
                        "name": parts[1].strip(),
                        "sector": safe_str(parts[47]),
                    }
                except:
                    continue
        except Exception as e:
            print(f"    Tencent batch error: {e}")
        time.sleep(0.2)
    return results

# ─── 涨停股 ────────────────────────────────────────────────────────────────
def fetch_limit_up():
    today = date.today().strftime("%Y%m%d")
    print(f"[涨停] fetching {today} ...")
    if not AKSHARE_OK:
        print("  -> akshare not available")
        return []
    
    try:
        df = ak.stock_zt_pool_em(date=today)
        if df is None or df.empty:
            print("  -> no data")
            return []
        print(f"  -> {len(df)} records")

        records = []
        for _, row in df.iterrows():
            code = safe_str(row.get("代码") or row.get("股票代码", ""))
            name = safe_str(row.get("名称") or row.get("股票名称", ""))
            if not code:
                continue
            
            raw_time = safe_str(row.get("首次封板时间") or "")
            limit_time = f"{raw_time[:2]}:{raw_time[2:4]}:{raw_time[4:6]}" if len(raw_time) >= 6 else raw_time
            
            records.append({
                "name": name,
                "code": code,
                "change_pct": safe_float(row.get("涨跌幅") or row.get("涨幅", 0)),
                "board_name": safe_str(row.get("所属行业") or row.get("行业", "")),
                "reason": safe_str(row.get("所属行业") or row.get("行业", "涨停")),
                "price": safe_float(row.get("最新价", 0)),
                "today_pct": safe_float(row.get("涨跌幅") or row.get("涨幅", 0)),
                "limit_time": limit_time,
                "continuous_limit_up_count": int(safe_float(row.get("连板数", 0))),
            })
        print(f"  -> {len(records)} valid")
        return records
    except Exception as e:
        print(f"  -> FAILED: {e}")
        return []

# ─── 月涨幅 TOP15 ─────────────────────────────────────────────────────────--
def fetch_month_top_v10(code_board_map):
    """
    月涨幅TOP15 - Sina财经K线版 v10b
    候选池策略（按优先级）：
    1. akshare全量A股按今日涨幅TOP300（理想情况）
    2. Fallback：涨停池 + 腾讯批量补充各行业龙头
    """
    print("[月涨幅 v10b] starting...")

    candidates = []
    
    # ── Step 1: akshare候选池（优先）───────────────────────────────────
    if AKSHARE_OK:
        try:
            df = ak.stock_zh_a_spot_em()
            if df is not None and not df.empty:
                print(f"  akshare spot: {len(df)} stocks")
                df = df.sort_values(by="涨跌幅", ascending=False)
                for _, row in df.head(300).iterrows():
                    code = safe_str(row.get("代码") or row.get("股票代码", ""))
                    name = safe_str(row.get("名称") or row.get("股票名称", ""))
                    today_pct = safe_float(row.get("涨跌幅") or row.get("涨幅", 0))
                    sector = safe_str(row.get("所属行业") or row.get("行业", ""))
                    
                    if not code or is_new_stock(name) or is_beijing_stock(code):
                        continue
                    
                    board = sector
                    if not board and code in code_board_map:
                        board = code_board_map[code]
                    
                    candidates.append({
                        "code": code,
                        "name": name,
                        "today_pct": today_pct,
                        "board": board,
                        "price": None,
                    })
                print(f"  -> akshare pool: {len(candidates)} stocks")
        except Exception as e:
            print(f"  akshare spot FAILED: {e}")

    # ── Step 2: Fallback候选池 ──────────────────────────────────────────
    if not candidates:
        print("  -> using fallback ZT + Tencent pool...")
        
        # 涨停池股票
        zt_codes = list(code_board_map.keys())
        zt_sh = [f"sh{c}" for c in zt_codes if c.startswith("6")]
        zt_sz = [f"sz{c}" for c in zt_codes if not c.startswith("6")]
        
        all_rt = {}
        for batch in [zt_sh, zt_sz]:
            if batch:
                all_rt.update(fetch_tengxun_realtime(batch))
        
        # ZT池 -> 候选
        for code, board in code_board_map.items():
            prefix = "sh" if code.startswith("6") else "sz"
            sym = f"{prefix}{code}"
            if sym in all_rt:
                info = all_rt[sym]
                candidates.append({
                    "code": code,
                    "name": info["name"],
                    "today_pct": info["change_pct"],
                    "board": board or info.get("sector", ""),
                    "price": info["price"],
                })
        
        # 腾讯批量补充：按涨幅分段采样（覆盖全市场）
        # 利用腾讯批量每批50只，分批获取不同涨幅区间
        sample_codes = []
        # 取涨停池附近的股票代码段来采样
        base_codes = [
            # 创业/深市高涨幅段
            "sz300970,sz300721,sz300590,sz300067,sz300061,sz300049,sz300095,sz300843",
            "sz300999,sz300896,sz300888,sz300985,sz300751,sz300760,sz300681,sz300529",
            "sz300059,sz300124,sz300274,sz300142,sz300759,sz300122,sz300496,sz300408",
            # 沪市高涨幅段
            "sh600052,sh600770,sh600736,sh600052,sh600310,sh600726,sh600396,sh600186",
            "sh600905,sh601016,sh600900,sh601318,sh600519,sh600036,sh601166,sh601398",
            # 科创板
            "sh688628,sh688051,sh688702,sh688116,sh688981,sh688012,sh688111,sh688396",
            "sh688095,sh688599,sh688521,sh688008,sh688126,sh688111,sh688036,sh688083",
        ]
        
        for batch_str in sample_codes:
            codes = batch_str.replace(" ", "").split(",")
            rt = fetch_tengxun_realtime(codes)
            for sym, info in rt.items():
                real_code = sym[2:]
                if is_new_stock(info["name"]) or is_beijing_stock(real_code):
                    continue
                if real_code not in [c["code"] for c in candidates]:
                    candidates.append({
                        "code": real_code,
                        "name": info["name"],
                        "today_pct": info["change_pct"],
                        "board": info.get("sector", ""),
                        "price": info["price"],
                    })
        
        # 按今日涨幅降序，取TOP100
        candidates.sort(key=lambda x: x.get("today_pct", 0), reverse=True)
        candidates = candidates[:100]
        print(f"  -> fallback pool: {len(candidates)} stocks")
    
    # ── Step 3: 腾讯批量获取所有候选的实时价格 ─────────────────────────
    codes_sh = [f"sh{c['code']}" for c in candidates if c["code"].startswith("6")]
    codes_sz = [f"sz{c['code']}" for c in candidates if c["code"].startswith("0")
                or c["code"].startswith("3")]
    
    all_rt = {}
    for batch_codes in [codes_sh, codes_sz]:
        if batch_codes:
            all_rt.update(fetch_tengxun_realtime(batch_codes))
    
    for c in candidates:
        code = c["code"]
        prefix = "sh" if code.startswith("6") else "sz"
        sym = f"{prefix}{code}"
        if sym in all_rt:
            c["price"] = all_rt[sym]["price"]
            if not c["board"]:
                c["board"] = all_rt[sym]["sector"]
    
    no_price = sum(1 for c in candidates if c["price"] is None or c["price"] <= 0)
    print(f"  -> realtime price: {len(candidates) - no_price}/{len(candidates)} matched")
    
    # ── Step 4: Sina K线计算月涨幅 ────────────────────────────────────
    results = []
    no_kline = 0
    processed = 0
    
    for i, c in enumerate(candidates):
        code = c["code"]
        price = c["price"]
        
        if price is None or price <= 0:
            continue
        
        month_pct, base_price, first_date = calc_month_pct(code, price)
        
        if month_pct is None:
            no_kline += 1
            month_pct = c["today_pct"]
            base_price = price / (1 + c["today_pct"] / 100) if c["today_pct"] != 0 else price
        else:
            print(f"    {c['name']}({code}): base={base_price}@{first_date} -> month_pct={month_pct}%")
        
        results.append({
            "code": code,
            "name": c["name"],
            "price": price,
            "pct": month_pct,
            "today_pct": c["today_pct"],
            "base_price": base_price,
            "board": c["board"],
        })
        
        processed += 1
        
        if processed >= 80:
            break
        
        if (i + 1) % 20 == 0:
            print(f"  -> {i+1}/{min(len(candidates), 300)}, got={len(results)}, no_kline={no_kline}")
        
        time.sleep(0.15)
    
    print(f"  -> done: {len(results)} processed, no_kline={no_kline}")
    
    # ── Step 5: 排序取TOP15 ───────────────────────────────────────────
    results.sort(key=lambda x: x["pct"], reverse=True)
    
    for r in results[:3]:
        calc = round((r["price"] - r["base_price"]) / r["base_price"] * 100, 2) if r["base_price"] > 0 else 0
        ok = "OK" if abs(calc - r["pct"]) < 0.01 else "FAIL"
        print(f"    [{ok}] {r['name']}: pct={r['pct']}% base={r['base_price']} price={r['price']}")
    
    top15 = []
    for i, r in enumerate(results[:15]):
        top15.append({
            "rank": i + 1,
            "name": r["name"],
            "code": r["code"],
            "pct": r["pct"],
            "price": round(r["price"], 2),
            "board": r["board"],
            "reason": r["board"] or "强势",
            "today_pct": r["today_pct"],
            "base_price": round(r["base_price"], 2),
        })
    
    return top15

# ─── 板块数据 ──────────────────────────────────────────────────────────────
def fetch_sectors(zt_list):
    print("[板块] fetching ...")

    board_limit_up_count = {}
    for stock in zt_list:
        board = stock.get("board_name", "")
        if board:
            board_limit_up_count[board] = board_limit_up_count.get(board, 0) + 1

    print(f"  -> limit_up: {len(board_limit_up_count)} boards")
    for b, c in sorted(board_limit_up_count.items(), key=lambda x: x[1], reverse=True)[:8]:
        print(f"    {b}: {c}只")

    sectors = []
    matched_boards = set()

    if AKSHARE_OK:
        try:
            df = ak.stock_sector_spot()
            if df is not None and not df.empty:
                df = df.sort_values(by="涨跌幅", ascending=False)
                for _, row in df.head(32).iterrows():
                    name = safe_str(row.get("板块") or row.get("名称") or "")
                    if not name:
                        continue
                    pct = safe_float(row.get("涨跌幅") or row.get("涨幅") or 0)
                    limit_count = board_limit_up_count.get(name, 0)
                    sectors.append({
                        "name": name,
                        "score": 30,
                        "pct": round(pct, 2),
                        "main_net": 0.0,
                        "limit_up_count": limit_count,
                        "phase": "",
                        "max_continuous": 0,
                    })
                    if limit_count > 0:
                        matched_boards.add(name)
        except Exception as e:
            print(f"  -> sector_spot FAILED: {e}")

    for board_name, cnt in sorted(board_limit_up_count.items(), key=lambda x: x[1], reverse=True):
        if board_name not in matched_boards:
            stocks_in = [s for s in zt_list if s.get("board_name") == board_name]
            total_pct = sum(s["change_pct"] for s in stocks_in)
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

# ─── 主程序 ────────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n=== Refresh v10b {now} ===")

    zt_list = fetch_limit_up()

    code_board_map = {s["code"]: s["board_name"] for s in zt_list if s["code"]}
    print(f"[主] zt code->board: {len(code_board_map)} entries")

    month_top = fetch_month_top_v10(code_board_map)

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
