#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GitHub Actions 数据刷新脚本 v12
- 数据源全部使用东方财富push2 API + Sina日K线 + 腾讯实时（无需akshare）
- 东方财富全量A股按涨幅排序（分页获取）
- 月涨幅：Sina日K线，4月首日开盘价 vs 当前价
"""
import json, time, os, sys, urllib.request
from datetime import date, datetime

import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def safe_float(v, default=0.0):
    try:
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        if s in ('-', '', 'nan', 'None'):
            return default
        f = float(s)
        return default if str(f) in ('nan', 'inf') else f
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
    return c.startswith("8") or c.startswith("4") or c.startswith("92") or c.startswith("93") or c.startswith("94")

def http_get(url, timeout=12):
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Referer": "https://quote.eastmoney.com/"
            })
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            if attempt < 2:
                time.sleep(0.5)
    return None

# ─── 东方财富全量A股（按涨幅降序）───────────────────────────────────────────────
def fetch_all_stocks_em():
    """
    用东方财富行情接口获取全量A股，按今日涨幅降序。
    fs=m:0+t:6(沪主板)+m:0+t:13(科创)+m:1+t:2(深主板)+m:1+t:23(创业板)
    f2=最新价, f3=涨跌幅, f4=涨跌额, f12=代码, f14=名称
    返回: [{code, name, price, change_pct, yesterday_close}, ...]
    """
    print("[东方财富] fetching all A-stocks by change%...")
    all_stocks = []
    total = None
    
    for page in range(1, 100):
        ts = int(time.time() * 1000)
        url = (f"https://push2.eastmoney.com/api/qt/clist/get"
               f"?pn={page}&pz=50&po=1&np=1&fltt=2&invt=2&fid=f3"
               f"&fs=m:0+t:6,m:0+t:13,m:1+t:2,m:1+t:23"
               f"&fields=f2,f3,f4,f12,f14&_={ts}")
        
        raw = http_get(url)
        if not raw:
            print(f"  page {page}: network error, stop")
            break
        
        try:
            d = json.loads(raw)
            diff = d.get("data", {}).get("diff", [])
            if total is None:
                total = d.get("data", {}).get("total", 0)
                print(f"  total stocks: {total}")
            if not diff:
                print(f"  page {page}: empty, done")
                break
            for item in diff:
                code = str(item.get("f12", ""))
                name = safe_str(item.get("f14", ""))
                price = safe_float(item.get("f2", 0))
                pct = safe_float(item.get("f3", 0))
                if not code or is_new_stock(name) or is_beijing_stock(code):
                    continue
                if price <= 0:
                    continue
                # 涨跌幅字段f3需要除100
                pct = pct / 100 if abs(pct) > 100 else pct  # 如果数值>100说明是百分比的百分之一形式
                yesterday_close = round(price / (1 + pct / 100), 2) if abs(pct) > 0.001 else price
                
                all_stocks.append({
                    "code": code,
                    "name": name,
                    "price": price,  # f2直接是元，不用除100（已有判断price>100的逻辑会误伤科创高价股）
                    "change_pct": round(pct, 2),
                    "yesterday_close": yesterday_close,
                    "sector": "",
                })
            
            if page % 10 == 0:
                print(f"  page {page}: collected {len(all_stocks)}")
            
            if len(diff) < 50:
                break
            
            time.sleep(0.08)
        except Exception as e:
            print(f"  page {page}: parse error {e}")
            break
    
    # 按今日涨幅降序
    all_stocks.sort(key=lambda x: x["change_pct"], reverse=True)
    print(f"  -> {len(all_stocks)} stocks (sorted by change%)")
    return all_stocks

# ─── 涨停池 ──────────────────────────────────────────────────────────────────
def build_zt_pool(stocks):
    """从全量A股中筛选涨停股（今日涨幅 >= 9.9%，非ST）"""
    print("[涨停池] filtering ZT stocks...")
    zt_list = []
    for s in stocks:
        pct = s["change_pct"]
        name = s["name"]
        is_st = any(k in name for k in ["ST", "S*", "*S", "退"])
        is_zt = (pct >= 9.9 and not is_st) or (pct >= 4.9 and is_st)
        if not is_zt:
            continue
        
        zt_list.append({
            "name": name,
            "code": s["code"],
            "change_pct": pct,
            "board_name": s["sector"],
            "reason": s["sector"] or "涨停",
            "price": s["price"],
            "today_pct": pct,
            "limit_time": "09:30:00",
            "continuous_limit_up_count": 1,
        })
    
    zt_list.sort(key=lambda x: x["change_pct"], reverse=True)
    print(f"  -> {len(zt_list)} ZT stocks")
    for z in zt_list[:5]:
        print(f"    {z['name']}({z['code']}): {z['change_pct']}%")
    return zt_list

# ─── Sina财经日K线 ────────────────────────────────────────────────────────────
def fetch_sina_kline(code, count=60):
    """获取Sina财经日K，返回[(date_str, open, close), ...] 升序"""
    c = str(code)
    prefix = "sz" if not c.startswith("6") else "sh"
    sym = f"{prefix}{c}"
    url = (f"https://money.finance.sina.com.cn/quotes_service/api/json_v2.php"
           f"/CN_MarketData.getKLineData?symbol={sym}&scale=240&ma=no&datalen={count}")
    
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=12) as resp:
                raw = resp.read().decode("utf-8")
            if not raw or raw.strip() == "":
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
        except:
            if attempt < 2:
                time.sleep(0.5)
    return []

def calc_month_pct(code, current_price):
    """
    用Sina财经K线计算月涨幅（4月基准）
    找4月份第一个交易日开盘价
    返回: (month_pct, base_price, first_april_date) 或 (None, None, None)
    """
    klines = fetch_sina_kline(code, 60)
    if not klines:
        return None, None, None
    
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
    """腾讯实时行情批量，返回{symbol: {price, change_pct, name, sector}}"""
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
        time.sleep(0.12)
    return results

# ─── 月涨幅 TOP15 ───────────────────────────────────────────────────────────
def fetch_month_top(stocks):
    """
    月涨幅TOP15 - v12
    候选池：全量A股按今日涨幅TOP300
    """
    print("[月涨幅] starting...")
    
    # 候选池：取今日涨幅TOP300
    candidates = []
    for s in stocks:
        code = s["code"]
        name = s["name"]
        if is_new_stock(name) or is_beijing_stock(code):
            continue
        if s["price"] <= 0:
            continue
        candidates.append({
            "code": code,
            "name": name,
            "today_pct": s["change_pct"],
            "board": s["sector"],
            "price": s["price"],
        })
        if len(candidates) >= 300:
            break
    
    print(f"  candidate pool: {len(candidates)} stocks")
    
    # 腾讯批量获取实时价格
    codes_sh = [f"sh{c['code']}" for c in candidates if c["code"].startswith("6")]
    codes_sz = [f"sz{c['code']}" for c in candidates if not c["code"].startswith("6")]
    
    all_rt = {}
    for batch in [codes_sh, codes_sz]:
        if batch:
            all_rt.update(fetch_tengxun_realtime(batch))
    
    for c in candidates:
        code = c["code"]
        prefix = "sh" if code.startswith("6") else "sz"
        sym = f"{prefix}{code}"
        if sym in all_rt:
            c["price"] = all_rt[sym]["price"]
            if not c["board"] or c["board"] == "":
                c["board"] = all_rt[sym]["sector"]
    
    # 计算月涨幅
    results = []
    no_kline = 0
    
    for i, c in enumerate(candidates):
        code = c["code"]
        price = c["price"]
        if price <= 0:
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
        
        if len(results) >= 150:
            print(f"  -> processed {len(results)}, stopping at cap")
            break
        
        if (i + 1) % 50 == 0:
            print(f"  -> {i+1}/{len(candidates)}, got={len(results)}, no_kline={no_kline}")
        
        time.sleep(0.12)
    
    print(f"  -> done: {len(results)} processed, no_kline={no_kline}")
    
    # 排序取TOP15
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
    """板块数据：涨停池统计 + 东方财富板块涨幅"""
    print("[板块] building...")
    
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
    
    # 东方财富板块排行（申万行业）
    # sw_a = 申万行业, node=1 是一级行业
    for page in range(1, 5):
        ts = int(time.time() * 1000)
        url = (f"https://push2.eastmoney.com/api/qt/clist/get"
               f"?pn={page}&pz=50&po=1&np=1&fltt=2&invt=2&fid=f3"
               f"&fs=m:90+t:2&fields=f2,f3,f4,f12,f14&_={ts}")
        
        raw = http_get(url)
        if not raw:
            break
        try:
            d = json.loads(raw)
            diff = d.get("data", {}).get("diff", [])
            if not diff:
                break
            for item in diff:
                name = safe_str(item.get("f14", ""))
                pct = safe_float(item.get("f3", 0)) / 100
                if not name:
                    continue
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
            if len(diff) < 50:
                break
        except Exception as e:
            print(f"  -> sector page {page} error: {e}")
            break
    
    # 补充涨停池中有但API没有的板块
    for board_name, cnt in sorted(board_limit_up_count.items(), key=lambda x: x[1], reverse=True):
        if board_name not in matched_boards:
            stocks_in = [s for s in zt_list if s.get("board_name") == board_name]
            avg_pct = sum(s["change_pct"] for s in stocks_in) / len(stocks_in) if stocks_in else 0
            sectors.append({
                "name": board_name,
                "score": 30,
                "pct": round(avg_pct, 2),
                "main_net": 0.0,
                "limit_up_count": cnt,
                "phase": "",
                "max_continuous": 0,
            })
    
    sectors.sort(key=lambda x: (x["limit_up_count"], x["pct"]), reverse=True)
    print(f"  -> total {len(sectors)} sectors")
    return sectors[:40]

# ─── 主程序 ────────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n=== Refresh v12 {now} ===")
    
    stocks = fetch_all_stocks_em()
    zt_list = build_zt_pool(stocks)
    code_board_map = {s["code"]: s["board_name"] for s in zt_list if s["code"]}
    print(f"[主] zt code->board: {len(code_board_map)} entries")
    
    # 用腾讯实时价格覆盖涨停池（东方财富价格有误/分或厘格式问题）
    if zt_list:
        zt_codes = [f"sh{s['code']}" if s["code"].startswith("6") else f"sz{s['code']}"
                    for s in zt_list]
        all_rt_zt = {}
        for i in range(0, len(zt_codes), 50):
            batch = zt_codes[i:i+50]
            rt = fetch_tengxun_realtime(batch)
            all_rt_zt.update(rt)
        for item in zt_list:
            code = item["code"]
            prefix = "sh" if code.startswith("6") else "sz"
            sym = f"{prefix}{code}"
            if sym in all_rt_zt:
                rt_data = all_rt_zt[sym]
                if rt_data.get("price", 0) > 0:
                    item["price"] = rt_data["price"]
                if rt_data.get("change_pct") is not None:
                    item["change_pct"] = rt_data["change_pct"]
                if rt_data.get("sector"):
                    item["board_name"] = rt_data["sector"]
                    item["reason"] = rt_data["sector"] or "涨停"
        print(f"[主] zt real-time price updated for {len(all_rt_zt)} stocks")

    month_top = fetch_month_top(stocks)
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
    print(f"Done in {round(time.time()-t0, 1)}s")
    return data

if __name__ == "__main__":
    main()
