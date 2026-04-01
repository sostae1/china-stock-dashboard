# 开源 OpenClaw A 股数据插件：指数 / ETF / 个股 / 期权统一接口，已上架 ClawHub


做 A 股 / ETF / 挂牌期权数据的朋友，最近我把插件 `openclaw-data-china-stock`（v0.1.2）正式维护在 ClawHub 上了，MIT 开源。

如果你也在用 OpenClaw 或在搭 Agent / Workflow，这个插件的目标很直接：

把「取数据」这一步从零散脚本，变成可复用、可维护、可扩展的统一工具层。

---

## 这个插件解决什么问题？

很多人不是缺数据，而是缺一层稳定的数据底座：

- 多数据源字段/口径不一致，接入后很难统一
- 单接口限流或偶发故障，流程容易中断
- 缓存策略不清晰，容易出现“看起来有数据但不可信”的情况

我在插件里做了三件事：

1. **统一入口**：主推 `tool_fetch_market_data`
2. **多源优先级 + 自动降级**：减少单点故障
3. **缓存默认更稳妥**：`data_cache.enabled=false`（默认不写盘，需写入再显式开启）

---

## 能力范围（按工具清单持续演进）

- 资产：**指数 / ETF / 个股 / 挂牌期权**
- 视图：实时、历史、分钟、开盘、Greeks 等；**股票**在统一入口下另有 `timeshare`（分时）、`pre_market`、`market_overview`、`valuation_snapshot` 等扩展 `view`
- **A 股底座（P0）**：证券主数据、三大表财报、公司行为（分红/解禁/增发/配股/回购）、融资融券、大宗交易等独立 `tool_*`
- **A 股参考类（P1）**：股东与持股、新股 IPO 流水线、指数成份（可选权重）、个股新闻与研报
- 扩展工具：涨停、龙虎榜、北向资金、板块热度等（以 manifest 清单为准）
- 返回格式：多数为带 `success / data / message / source` 的 JSON；扩展工具常带 `fallback_route`、`attempt_counts`；可选 `provider_preference` 调整多源尝试顺序

---

## 安装方式（任选其一）

```bash
openclaw plugins install clawhub:@shaoxing-xie/openclaw-data-china-stock
```

```bash
openclaw plugins install @shaoxing-xie/openclaw-data-china-stock
```

安装后按你的环境重启 OpenClaw Gateway（或等价服务），再在 Dashboard / status 中确认插件已加载。

---

## 链接

- GitHub: <https://github.com/shaoxing-xie/openclaw-data-china-stock>
- ClawHub: <https://clawhub.ai/plugins/%40shaoxing-xie%2Fopenclaw-data-china-stock>

欢迎试用、提 Issue / PR，一起把这套 A 股数据工具链打磨得更稳。

> 免责声明：插件仅用于数据采集与技术研究，不构成任何投资建议。

---

## 发布时可附上的话题（提高检索）

`#OpenClaw` `#ClawHub` `#A股` `#ETF` `#期权` `#量化交易` `#Python` `#开源项目`

---

## 30 秒发布检查清单

- [ ] 标题包含关键词：OpenClaw / A股 / ETF / 期权 / ClawHub
- [ ] 正文前 5 行讲清“痛点 + 方案”
- [ ] 至少放 1 条可复制安装命令
- [ ] 放上 GitHub + ClawHub 双链接
- [ ] 末尾加免责声明
