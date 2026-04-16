with open(r'C:\Users\Administrator\.qclaw\workspace\china-stock-data\backend.py', encoding='utf-8') as f:
    lines = f.readlines()

keywords = ['month_pct', '月涨幅', '本月', 'first_k', '月度', 'monthly']
for i, line in enumerate(lines, 1):
    for kw in keywords:
        if kw in line:
            print(f"{i}: {line.rstrip()}")
            break
