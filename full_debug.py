# 直接模拟后端逻辑
import subprocess
import json

def get_sector_heat():
    try:
        r = subprocess.run(
            ["python", r"C:\Users\Administrator\.qclaw\workspace\china-stock-data\tool_runner.py", "tool_sector_heat_score"],
            capture_output=True, timeout=30
        )
        if r.stdout:
            text = r.stdout.decode('gbk', errors='replace')
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
                print(f"get_sector_heat: {len(result)} sectors from tool")
                return result, sectors
    except Exception as e:
        print(f"get_sector_heat error: {e}")
    return {}, []

sector_map, sector_list = get_sector_heat()

print(f"\n=== sector_list 前15 ===")
for i, s in enumerate(sector_list[:15]):
    name = s.get('name', '')
    score = s.get('score', 0)
    pct = s.get('change_percent')
    avg = s.get('avg_change', 0)
    print(f'{i+1:2}. {name}: score={score}, change_percent={pct}, avg_change={avg}')

print(f"\n=== 处理后的 sector_hot ===")
sector_hot = []
for s in sector_list[:15]:
    pct = s.get("change_percent")
    if pct is None:
        pct = s.get("avg_change", 0)
    sector_hot.append({
        "name": s.get("name", ""),
        "score": s.get("score", 0),
        "pct": round(float(pct or 0), 2),
        "limit_up_count": s.get("limit_up_count", 0),
    })

for i, s in enumerate(sector_hot):
    print(f'{i+1:2}. {s["name"]}: pct={s["pct"]:+.2f}%, score={s["score"]}')
