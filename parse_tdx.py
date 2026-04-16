import struct, os
from datetime import date

# 实际正确路径
TDX = r'D:\通达信三点一线专用版'

def parse_day(fp):
    """解析.day文件"""
    if not os.path.exists(fp):
        return []
    bars = []
    with open(fp, 'rb') as f:
        while True:
            d = f.read(32)
            if len(d) < 32:
                break
            try:
                di, o, h, l, c = struct.unpack('>Iffff', d[:20])
            except:
                break
            if di < 20100101 or di > 20991231 or c <= 0:
                continue
            y, r = divmod(di, 10000)
            m, day = divmod(r, 100)
            if day < 1 or day > 31:
                day = 1
            try:
                dt = date(int(y), int(m), int(day))
            except:
                continue
            bars.append({'date': dt, 'open': round(o, 2), 'close': round(c, 2)})
    return bars

fp = os.path.join(TDX, 'vipdoc', 'sz', 'lday', 'sz000586.day')
print('File:', fp)
print('Exists:', os.path.exists(fp))
bars = parse_day(fp)
print('Bars:', len(bars))
if bars:
    print('Last 3:', [(str(b['date']), b['open'], b['close']) for b in bars[-3:]])
    cur = bars[-1]['close']
    m = bars[-1]['date'].month
    mb = [b for b in bars if b['date'].month == m]
    print('This month bars:', len(mb), 'month:', m)
    if mb and mb[0]['open'] > 0:
        pct = round((cur - mb[0]['open']) / mb[0]['open'] * 100, 2)
        print('Month pct:', pct, '% (open', mb[0]['open'], '-> close', cur, ')')
