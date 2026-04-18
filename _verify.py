import json

with open('snapshot_v5.json', 'r', encoding='utf-8') as f:
    d = json.load(f)

print('sector_hot:')
for s in d['sector_hot'][:5]:
    name = s['name']
    pct = s['pct']
    score = s['score']
    print(f'  {name}: pct={pct}, score={score}')
print(f'...\ntotal sectors: {len(d["sector_hot"])}')
print(f'total zt: {len(d["zt_today"])}')
print(f'total month: {len(d["month_top"])}')
