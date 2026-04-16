import json, os, time, concurrent.futures, urllib.request, subprocess
from datetime import date

# ─── K线 & 月涨幅 ─────────────────────────────────────────────
def get_qq_kline(code, count=120):
    """从腾讯API获取K线，返回[{date, open, close}]，日期升序"""
    mk = "sh" if code.startswith("6") else "sz"
    url = (f"http://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
           f"?_var=kline_dayqfq&param={mk}{code},day,,,{count},qfq")
    try:
        r = urllib.request.urlopen(url, timeout=6)
        raw = r.read().decode('utf-8')
        if '(' in raw:
            js = raw.split('(', 1)[1].rstrip(')')
        else:
            js = raw.split('=', 1)[1]
        d = json.loads(js)
        raw_bars = d['data'][f'{mk}{code}']['qfqday']
        bars = []
        for b in raw_bars:
            try:
                bars.append({
                    "date": date.fromisoformat(str(b[0])),
                    "open": round(float(b[1]), 2),
                    "close": round(float(b[2]), 2)
                })
            except:
                pass
        return bars
    except:
        return []

def calc_month_pct(bars):
    """月涨幅 = (最新收盘 - 当月1日开盘) / 当月1日开盘 * 100"""
    if not bars or len(bars) < 2:
        return None, None, None, False
    cur_close = bars[-1]["close"]
    this_y, this_m = bars[-1]["date"].year, bars[-1]["date"].month

    # 找当月第一根K线的开盘价
    first_open = None
    for b in bars:
        if b["date"].year == this_y and b["date"].month == this_m:
            first_open = b["open"]
            break

    # 次新股：本月数据少于3根
    month_bars = [b for b in bars if b["date"].month == this_m]
    if first_open is None or first_open <= 0 or len(month_bars) < 3:
        return None, None, None, False

    pct = round((cur_close - first_open) / first_open * 100, 2)
    return pct, cur_close, first_open, False

# ─── 实时价格 ─────────────────────────────────────────────────
def get_realtime(code):
    """腾讯实时行情：价格 + 今日涨幅"""
    mk = "sh" if code.startswith("6") else "sz"
    try:
        r = urllib.request.urlopen(f"http://qt.gtimg.cn/q={mk}{code}", timeout=3)
        parts = r.read().decode('gbk', errors='ignore').split('~')
        if len(parts) > 32:
            return round(float(parts[3]), 2) if parts[3] else 0, \
                   round(float(parts[32]), 2) if parts[32] else 0
    except:
        pass
    return None, None

# ─── 涨停原因（腾讯行情标签 parts[60] + 插件board_name备选）──
def get_zt_reason(code):
    """从腾讯实时行情 parts[60] 拿概念标签（题材）"""
    mk = "sh" if code.startswith("6") else "sz"
    try:
        r = urllib.request.urlopen(f"http://qt.gtimg.cn/q={mk}{code}", timeout=3)
        parts = r.read().decode("gbk", errors="ignore").split("~")
        # parts[60]=AR/AI/VR等概念标签，parts[61]=GP-A-CYB(市场类型)
        tag = parts[60].strip() if len(parts) > 60 else ""
        # 过滤无意义的标签（单字母、短标签、常见缩写）
        INVALID_TAGS = {"A", "R", "AR", "AI", "VR", "GP", "G", "N", "C", "ST", "*ST", "A R", "A I", "V R"}
        if tag and tag not in INVALID_TAGS and len(tag) > 2:
            # 再检查是否全是单字母组合（如"A R C"）
            words = tag.split()
            if all(len(w) <= 1 for w in words):
                return ""
            return tag
    except:
        pass
    return ""

# ─── 涨停数据 ─────────────────────────────────────────────────
def get_zt_from_plugin():
    import subprocess
    TOOL = r"C:\Users\Administrator\.qclaw\workspace\china-stock-data\tool_runner.py"
    try:
        r = subprocess.run(["python", TOOL, "tool_fetch_limit_up_stocks"],
                        capture_output=True, timeout=20)
        if r.stdout:
            # tool_runner.py outputs GBK on Windows, decode explicitly
            text = r.stdout.decode('gbk', errors='replace')
            d = json.loads(text)
            if d.get("success"):
                return d.get("data", [])
    except Exception as e:
        print(f"  get_zt_from_plugin error: {e}")
    return []

# ─── 热门板块（从tool_sector_heat_score获取）────────────────
def get_sector_heat():
    """从tool_sector_heat_score获取板块评分数据，返回(映射,列表)"""
    try:
        r = subprocess.run(
            ["python", r"C:\Users\Administrator\.qclaw\workspace\china-stock-data\tool_runner.py", "tool_sector_heat_score"],
            capture_output=True, timeout=30
        )
        if r.stdout:
            text = r.stdout.decode('utf-8', errors='replace')
            d = json.loads(text)
            if d.get("success"):
                sectors = d.get("sectors", [])
                result = {}
                for s in sectors:
                    name = s.get("name", "")
                    if name:
                        result[name] = {
                            "score": s.get("score", 0),
                            "limit_up_count": s.get("limit_up_count", 0),
                            "avg_change": s.get("avg_change", 0),
                            "net_flow": s.get("net_flow", 0),
                            "phase": s.get("phase", ""),
                            "max_continuous": s.get("max_continuous", 0),
                            "leaders": s.get("leaders", [])
                        }
                print(f"  get_sector_heat: {len(result)} sectors from tool")
                return result, sectors
    except Exception as e:
        print(f"  get_sector_heat error: {e}")
    return {}, []

# ─── 主逻辑 ────────────────────────────────────────────────────
def run():
    t0 = time.time()
    now = time.strftime("%Y-%m-%d %H:%M:%S")

    zt_raw = get_zt_from_plugin()
    print(f"[{now}] ZT from plugin: {len(zt_raw)}")

    # ── 构建股票池（去重）────────────────────────────────────
    pool = {}
    zt_list = []
    for s in zt_raw[:80]:
        code = s.get("code", "")
        name = s.get("name", "") or code
        board = s.get("board_name", "") or ""
        if not code or code in pool:
            continue
        pool[code] = {"n": name, "board": board, "lp": round(float(s.get("latest_price") or 0), 2)}

    # ── 并行：月涨幅 + 涨停原因 ──────────────────────────────
    results = []
    def job(code):
        bars = get_qq_kline(code, 120)
        pct, cur, base, new = calc_month_pct(bars)
        reason = get_zt_reason(code) or pool[code]["board"]
        return {"code": code, "pct": pct, "price": cur, "base": base,
                "reason": reason, "new": new}

    pex = concurrent.futures.ThreadPoolExecutor(max_workers=15)
    futs = {pex.submit(job, c): c for c in list(pool.keys())}
    for fu in concurrent.futures.as_completed(futs, timeout=60):
        r = fu.result()
        if r and r["pct"] is not None and not r["new"]:
            results.append(r)
    pex.shutdown(wait=False)

    # ── 合并实时价格 ────────────────────────────────────────
    def job_price(code):
        price, today_pct = get_realtime(code)
        return code, price, today_pct

    px = concurrent.futures.ThreadPoolExecutor(max_workers=15)
    pfuts = {px.submit(job_price, c): c for c in list(pool.keys())}
    price_map = {}
    for fu in concurrent.futures.as_completed(pfuts, timeout=15):
        code, price, today_pct = fu.result()
        if price and price > 0:
            price_map[code] = {"price": round(price, 2), "today_pct": round(today_pct, 2)}
    px.shutdown(wait=False)

    results.sort(key=lambda x: x["pct"], reverse=True)
    month = []
    for r in results[:15]:
        code = r["code"]
        inf = pool.get(code, {})
        pm = price_map.get(code, {})
        month.append({
            "rank": len(month) + 1,
            "name": inf.get("n", code),
            "code": code,
            "pct": r["pct"],
            "price": pm.get("price", r["price"]),
            "board": inf.get("board", "月涨幅"),
            "reason": r["reason"],
            "today_pct": pm.get("today_pct", 0),
            "base_price": r["base"]
        })

    # ── 构造涨停列表（保留所有原始字段）─────────────────────
    # 先获取板块评分数据用于增强涨停原因
    sector_map, sector_list = get_sector_heat()

    for s in zt_raw[:80]:
        code = s.get("code", "")
        if not code or code in {x["code"] for x in zt_list}:
            continue
        inf = pool.get(code, {})
        board = inf.get("board", "")

        # 增强涨停原因：板块名称 + 阶段（如有）
        tag = get_zt_reason(code) if code in pool else ""
        sec_info = sector_map.get(board, {})
        phase = sec_info.get("phase", "")
        if tag:
            reason = tag
        elif phase:
            reason = f"{board}·{phase}"
        else:
            reason = board or "涨停"

        zt_list.append({
            "name": inf.get("n", s.get("name", "")),
            "code": code,
            "change_pct": round(float(s.get("change_pct") or 0), 2),
            "board_name": board,
            "reason": reason,
            "price": round(float(s.get("latest_price") or 0), 2),
            "today_pct": round(float(s.get("change_pct") or 0), 2),
            "limit_time": s.get("limit_up_time", ""),
            "continuous_limit_up_count": s.get("continuous_limit_up_count", 0)
        })

    # ── 热门板块（从tool_sector_heat_score获取真实数据）────
    sector_hot = []
    for s in sector_list[:15]:
        # 使用 change_percent（板块真实涨幅），若无则用 avg_change（涨停股平均涨幅）
        pct = s.get("change_percent")
        if pct is None:
            pct = s.get("avg_change", 0)
        sector_hot.append({
            "name": s.get("name", ""),
            "score": s.get("score", 0),
            "pct": round(float(pct or 0), 2),
            "main_net": round(float(s.get("net_flow") or 0) / 1e8, 2),  # 转为亿元
            "limit_up_count": s.get("limit_up_count", 0),
            "phase": s.get("phase", ""),
            "max_continuous": s.get("max_continuous", 0)
        })
    print(f"  sector from tool: {len(sector_hot)} sectors, top={sector_hot[:3] if sector_hot else []}")

    print(f"Done in {round(time.time()-t0,1)}s | month:{len(month)} zt:{len(zt_list)} sector:{len(sector_hot)}")
    for s in month[:10]:
        print(f"  {s['rank']:2}. {s['name']}({s['code']}) 月涨:{s['pct']:6.2f}% 基准:{s['base_price']} 今:{s['today_pct']:+.2f}%")

    return {"updated_at": now, "month_top": month,
            "zt_today": zt_list, "sector_hot": sector_hot}

if __name__ == "__main__":
    data = run()
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "snapshot_v5.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("Written to", out)
