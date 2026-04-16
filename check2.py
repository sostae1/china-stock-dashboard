import urllib.request, json, time
time.sleep(2)
r = urllib.request.urlopen('http://127.0.0.1:5001/snapshot_v5.json', timeout=5)
d = json.loads(r.read())
print('月涨幅TOP10 (基准: 4月1日收盘价):')
for s in d['month_top'][:10]:
    print(f"{s['rank']}. {s['name']} {s['code']} 月涨:{s['pct']}% 今日:{s['today_pct']}% 价格:{s['price']}")
