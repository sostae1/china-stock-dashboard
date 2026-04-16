import urllib.request, json
r = urllib.request.urlopen('http://127.0.0.1:5001/snapshot_v5.json', timeout=5)
d = json.loads(r.read())
print(f"updated: {d['updated_at']} | month_top: {len(d['month_top'])}")
for s in d['month_top'][:8]:
    print(f"  {s['rank']}. {s['name']} 月涨:{s['pct']}% 今涨:{s['today_pct']}%")
