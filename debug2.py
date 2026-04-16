import json, os
f = os.path.join(r'C:\Users\Administrator\.qclaw\workspace\china-stock-data', 'snapshot_v2.json')
if os.path.exists(f):
    data = json.load(open(f, encoding='utf-8'))
    print('MTOP:', len(data.get('month_top',[])))
    for s in data.get('month_top',[])[:5]:
        print(' ', s)
    print('ZT:', len(data.get('zt_today',[])))
    for s in data.get('zt_today',[])[:3]:
        print(' ', s)
else:
    print('File not found')
