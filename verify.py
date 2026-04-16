import json
with open(r"C:\Users\Administrator\.qclaw\workspace\china-stock-data\snapshot_v5.json", encoding="utf-8") as f:
    d = json.load(f)

print("=== 涨停原因测试 ===")
for s in d["zt_today"][:5]:
    print(f"{s['name']}({s['code']}): reason=[{s.get('reason','空')}]")

print()
print("=== 板块TOP5 ===")
for s in d["sector_hot"][:5]:
    print(f"  {s['name']}: {s['count']}只涨停")

print()
print("=== 月涨幅TOP5（含基准价）===")
for s in d["month_top"][:5]:
    print(f"  {s['name']} 月涨{s['pct']}% 基准{s.get('base_price','?')}元")
