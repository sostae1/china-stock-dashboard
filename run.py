import json, os, time, struct, concurrent.futures, subprocess
from datetime import date

TOOL = r"C:\Users\Administrator\.qclaw\workspace\china-stock-data\tool_runner.py"
TDX = r"D:\通达信三点一线专用版"
VIPDOC = os.path.join(TDX, "vipdoc")

def plugin_call(tool):
    try:
        r = subprocess.run(["python", TOOL, tool], capture_output=True, text=True, timeout=20)
        if r.stdout:
            d = json.loads(r.stdout)
            if d.get("success"):
                return d.get("data", [])
    except:
        pass
    return []

def parse_tdx(fp):
    if not os.path.exists(fp):
        return []
    bars = []
    with open(fp, "rb") as f:
        while True:
            d = f.read(32)
            if len(d) < 32:
                break
            try:
                di_be = struct.unpack(">I", d[:4])[0]
                o, h, l, c = struct.unpack("<ffff", d[4:20])
            except:
                break
            if di_be < 20100101 or di_be > 20991231 or c <= 0:
                continue
            y, tmp = divmod(di_be, 10000)
            m, day = divmod(tmp, 100)
            if day < 1 or day > 31:
                day = 1
            try:
                dt = date(int(y), int(m), int(day))
            except:
                continue
            bars.append({"date": dt, "open": round(o, 2), "close": round(c, 2)})
    return bars

def month_pct(bars):
    if not bars or len(bars) < 2:
        return None, None, False
    cur = bars[-1]["close"]
    this_m = bars[-1]["date"].month
    mb = [b for b in bars if b["date"].month == this_m]
    if not mb:
        return None, None, False
    mo = mb[0]["open"]
    if mo <= 0:
        return None, None, False
    return round((cur - mo) / mo * 100, 2), cur, len(mb) < 3

def main():
    t0 = time.time()
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    
    zt = plugin_call("tool_fetch_limit_up_stocks")
    print("涨停:", len(zt), "只")
    
    info = {}
    zt_list = []
    for s in zt[:50]:
        code = s.get("code", "")
        name = s.get("name", "") or code
        board = s.get("board_name", "") or ""
        if not code or code in info:
            continue
        lp = round(float(s.get("latest_price") or 0), 2)
        tp = round(float(s.get("change_pct") or 0), 2)
        info[code] = {"n": name, "b": board, "p": lp, "tp": tp,
                       "ct": s.get("continuous_limit_up_count", 0),
                       "lt": s.get("limit_up_time", "")}
        zt_list.append({"name": name, "code": code,
                       "pct": round(float(s.get("change_pct") or 0), 0),
                       "board": board, "reason": board or "涨停",
                       "price": lp, "today_pct": tp,
                       "continuous": s.get("continuous_limit_up_count", 0),
                       "limit_time": s.get("limit_up_time", "")})
    
    results = []
    def job(code):
        mk = "sh" if code.startswith("6") else "sz"
        fp = os.path.join(VIPDOC, mk, "lday", mk + code + ".day")
        bars = parse_tdx(fp)
        pct, cur, new = month_pct(bars)
        if pct is None or new:
            return None
        inf = info.get(code, {})
        return {"code": code, "pct": pct, "price": cur,
                "n": inf.get("n", code), "b": inf.get("b", ""),
                "tp": inf.get("tp", 0)}
    
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=8)
    futures = {pool.submit(job, c): c for c in list(info.keys())}
    for fu in concurrent.futures.as_completed(futures, timeout=30):
        r = fu.result()
        if r:
            results.append(r)
    pool.shutdown(wait=False)
    
    results.sort(key=lambda x: x["pct"], reverse=True)
    month_top = []
    seen = set()
    for r in results:
        if r["code"] in seen:
            continue
        seen.add(r["code"])
        inf = info.get(r["code"], {})
        month_top.append({"rank": len(month_top) + 1,
                        "name": inf.get("n", r["code"]),
                        "code": r["code"],
                        "pct": r["pct"],
                        "price": inf.get("p", r["price"]),
                        "board": inf.get("b", ""),
                        "reason": inf.get("b", "") or "月涨幅",
                        "today_pct": inf.get("tp", 0)})
    
    print(f"Done in {round(time.time()-t0,1)}s | {now}")
    for s in month_top[:10]:
        print(f"  {s['rank']}. {s['name']}({s['code']}) 月涨{s['pct']}% 今涨{s.get('today_pct',0)}% [{s.get('board','')}]")
    print(f"涨停: {len(zt_list)}只")
    
    return {"updated_at": now, "month_top": month_top[:15], "zt_today": zt_list}

if __name__ == "__main__":
    data = main()
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "snapshot.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("Saved:", out)
