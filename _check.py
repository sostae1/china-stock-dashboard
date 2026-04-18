import json
with open('snapshot_v5.json','r',encoding='utf-8') as f:
    d=json.load(f)

print('=== 板块涨幅 ===')
for s in d['sector_hot'][:8]:
    pct = s['pct'] if s['pct'] is not None else 'N/A'
    print(f"  {s['name']}: {pct}% (score={s['score']})")

print(f"\n=== 涨停股(前5) ===")
for z in d['zt_today'][:5]:
    print(f"  {z['name']}({z['code']}): {z['change_pct']}% - {z['reason']}")

print(f"\n数据统计: 月涨幅{len(d['month_top'])}只, 涨停{len(d['zt_today'])}只, 板块{len(d['sector_hot'])}个")
