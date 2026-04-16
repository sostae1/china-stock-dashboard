import json, os
f = os.path.join(r'C:\Users\Administrator\.qclaw\workspace\china-stock-data', 'snapshot_new.json')
data = json.load(open(f, encoding='utf-8'))
print('Time:', data['updated_at'])
print('Month TOP:')
for s in data['month_top'][:10]:
    print('  ' + str(s['rank']) + '. ' + s['name'] + '(' + s['code'] + ') 月涨' + str(s['pct']) + '% 板=' + s.get('board',''))
print()
print('ZT today:')
for s in data['zt_today'][:10]:
    print('  ' + s['name'] + '(' + s['code'] + ') ' + str(s['pct']) + '% ' + s['board'] + ' 连板' + str(s['continuous']))
