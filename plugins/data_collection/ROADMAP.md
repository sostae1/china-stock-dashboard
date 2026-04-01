# data_collection 插件路线图与数据契约

本文档与 [README.md](./README.md) **互指**：README 侧重工具用法与示例；本文档侧重 **OpenClaw 分类**、**Provider 双层降级**、**不支持项**与 **DTO 字段约定**。

## A 股扩展工具分层（速查）

| 分层 | 模块入口 | 对外 `tool_*` | 说明 |
|------|-----------|---------------|------|
| P0 | `stock/fundamentals_extended.py` | `tool_fetch_a_share_universe`、`tool_fetch_stock_financial_reports`、`tool_fetch_stock_corporate_actions`、`tool_fetch_margin_trading`、`tool_fetch_block_trades` | 主数据、三大表、公司行为、两融、大宗；部分支持 `provider_preference` |
| P1 | `stock/reference_p1.py` | `tool_fetch_stock_shareholders`、`tool_fetch_ipo_calendar`、`tool_fetch_index_constituents`、`tool_fetch_stock_research_news` | 股东/IPO/成份/新闻研报 |
| 股票扩展 view | `stock/unified_stock_views.py`（由 `merged/fetch_market_data` 调用） | 仅用 `tool_fetch_market_data`，`asset_type=stock`，`view`=`timeshare` 等 | 与日线/分钟 K 线工具并列，语义见仓库根 README |
| 偏好序 | `utils/provider_preference.py` | （入参，非独立工具） | `reorder_provider_chain` / `normalize_provider_preference` |

AkShare 主函数与兜底说明：**附录 F（P0+统一入口视图）、附录 G（P1）**。

## OpenClaw 分类框架

### 第一维：标的物（Underlying）

| 标的物 | 目录 / 模块 |
|--------|-------------|
| A 股股票 | `stock/` |
| ETF | `etf/` |
| 指数 | `index/` |
| 期权 | `option/` |
| 期货 / 外围 | `futures/`、`index/fetch_global.py` |
| 板块 / 情绪 | `sector.py`、`limit_up/`、`dragon_tiger.py` |
| 资金流向 | `northbound.py`、`capital_flow.py` |
| 基础 / 财务 | `financials.py` |
| 工具 | `utils/`、`tick/` |

### 第二维：数据域

| 数据域 | 含义 |
|--------|------|
| 基础数据 | 证券信息、财务快照、估值等 |
| 行情数据 | OHLCV、快照、盘口（按周期再分） |
| 板块数据 | 行业/概念涨跌与热度 |
| 资金流向数据 | 北向、个股主力散户等 |
| 其他 | 交易日、合约列表、可交易性等 |

### 第三维：行情周期（仅行情域）

日线（含周/月若扩展）、分钟线、实时快照（可选五档）。

## 双层降级（稳定性）

1. **跨软件包（Provider）**：对同一「标的物 + 数据域 + 周期」，按配置依次尝试 Mootdx、Baostock、AkShare、Tushare（日线 EOD 且通常置后）等。
2. **包内多路由（AkShare）**：新浪、东方财富、腾讯等接口顺序在实现中固定，并在返回中标注 `source` / `route`。

**说明**：商业源（如 iFin）当前**不实现**；远期若接入，在本文档单独立表。

## A 股股票 · 日线 Provider 顺序（实现目标）

高 → 低（与 `stock/fetch_historical.py` 对齐，以代码为准）：

1. 本地缓存（若命中完整区间）
2. **mootdx**（通达信，可选）
3. **Baostock**（无 token，长历史；无 TDX 时作为主要免费兜底之一）
4. AkShare **新浪** `stock_zh_a_daily`
5. AkShare **东财** `stock_zh_a_hist`
6. AkShare **腾讯** `stock_zh_a_hist_tx`
7. **Tushare** `pro.daily`（EOD，需 token，**置后**）

## A 股股票 · 分钟 Provider 顺序

1. 缓存  
2. **mootdx**  
3. AkShare 新浪分钟  
4. AkShare 东财 `stock_zh_a_hist_min_em`  
5. **efinance**（可选，参数 `minute_source_preference=efinance` 或自动回退时启用）

## A 股股票 · 实时 Provider 顺序（双层链入口）

实现见 `providers/stock_realtime.py`（`fetch_stock_realtime` 调用同一顺序）：

1. **mootdx**（通达信远程，非纯 HTTP）  
2. **东财五档** `stock_bid_ask_em`（可选深度，失败则跳过）  
3. **腾讯** `qt.gtimg.cn`  
4. AkShare **新浪** `stock_zh_a_spot`

## 不支持项（默认）

- 全市场 WebSocket 推送、交易所 **L2 逐笔**（需专门授权 / DataFeed）。
- 绑定某一商业终端文档表述；行业缩写（BD/HQ/HF 等）仅作附录对照。

## 附录 A：行业术语对照（非规范）

| 常见缩写 | 含义 |
|----------|------|
| BD | 基础数据 |
| HQ | 历史行情 |
| HF | 高频 / 分钟 |
| RQ | 实时行情 |

## 附录 B：最小返回字段（DTO · 文档层）

| 字段 | 说明 |
|------|------|
| `success` | 是否成功 |
| `message` | 人类可读信息 |
| `data` | 主数据（列表或字典） |
| `source` / `provider` | 实际使用的数据源或 Provider 名 |
| `count` | 条数（若适用） |

Pydantic 模型在 Provider 代码落地（Phase B）时与工具入参一并引入。

## 附录 C：ETF ↔ 指数 ↔ 期权标的映射

见 [config/symbol_mapping.yaml](./config/symbol_mapping.yaml)。代码侧可 `from plugins.data_collection.config import load_symbol_mapping`。用于 510300↔000300 等关联，避免魔法字符串。

## 附录 D：期权合约主数据

合约列表与到期月份以 [utils/get_contracts.py](./utils/get_contracts.py) 为准；完整「行权价阶梯 / 最后交易日」以交易所规则 + 缓存为准，详见 `option/README.md`。

## 附录 E：IOPV / 折溢价

已实现轻量工具：`etf/fetch_realtime.fetch_etf_iopv_snapshot` / `tool_fetch_etf_iopv_snapshot`（`fund_etf_spot_em`，含 IOPV 与折价率字段）。亦可结合 ETF 日线 + 指数日线间接估算，见 `etf/README.md`。

## 附录 F：A 股扩展能力卡片（P0 · AkShare 主路由）

实现见 `stock/fundamentals_extended.py`、`stock/unified_stock_views.py`。跨包兜底以路线图正文双层降级为准；下列为 **包内主函数** 与 **返回可观测字段**。

| 工具（语义） | AkShare 主函数 | 备选 / 说明 |
|-------------|----------------|-------------|
| `tool_fetch_a_share_universe` | `stock_info_a_code_name` | `stock_zh_a_spot_em`（列裁剪）；`provider_preference` 调整顺序 |
| `tool_fetch_stock_financial_reports` | `stock_*_sheet_by_report_em`（三张表） | `stock_financial_report_sina`；`provider_preference` |
| `tool_fetch_stock_corporate_actions` | `stock_dividend_cninfo` / `stock_restricted_release_queue_em` / `stock_qbzf_em` / `stock_allotment_cninfo` / `stock_repurchase_em` | 按 `action_kind` 分支 |
| `tool_fetch_margin_trading` | `stock_margin_sse` / `stock_margin_szse` / `stock_margin_detail_*` / `stock_margin_underlying_info_szse` | 按 `market`+`data_kind` |
| `tool_fetch_block_trades` | `stock_dzjy_*` | 按 `block_kind` |
| `tool_fetch_market_data` · stock · `timeshare` | `stock_intraday_em` | `stock_intraday_sina` |
| `tool_fetch_market_data` · stock · `pre_market` | `stock_zh_a_hist_pre_min_em` | — |
| `tool_fetch_market_data` · stock · `market_overview` | `stock_sse_summary` + `stock_szse_summary` | 任一失败仍可能返回部分 `data` |
| `tool_fetch_market_data` · stock · `valuation_snapshot` | 委托 `tool_fetch_stock_financials` | 与估值快照字段对齐 |

统一返回（新工具）：`success`、`message`、`data`、`source`、`provider`、`fallback_route`、`attempt_counts`（及可选 `count`）。

## 附录 G：A 股参考类扩展（P1 · `stock/reference_p1.py`）

| 工具 | AkShare 主函数（按参数分支） | 多源 / 说明 |
|------|------------------------------|-------------|
| `tool_fetch_stock_shareholders` | `stock_main_stock_holder` / `stock_circulate_stock_holder` / `stock_share_change_cninfo`+`stock_shareholder_change_ths` / `stock_fund_stock_holder` | 户数控 `holder_count`：`cninfo`↔`ths`，`provider_preference` |
| `tool_fetch_ipo_calendar` | `stock_ipo_declare_em`、`stock_new_ipo_cninfo`、`stock_ipo_review_em`、`stock_ipo_tutor_em`、`stock_ipo_info`、`stock_ipo_summary_cninfo` | 按 `ipo_kind` |
| `tool_fetch_index_constituents` | `index_stock_cons_weight_csindex`（可选）、`index_stock_cons_csindex`、`index_stock_cons_sina`、`index_stock_cons` | `provider_preference` 调整顺序 |
| `tool_fetch_stock_research_news` | `stock_news_em`、`stock_research_report_em`、`stock_news_main_cx` | 按 `content_kind` |

**L4 测试**：`tests/test_dto_snapshots_l4.py` + `tests/fixtures/l4/*.json` 锁定部分工具输出列名集合。

## 进度与下一步（Repo 工程化清单）

### 已完成（v0.1 起步阶段）
- 补齐 `requirements.txt` 的核心依赖，并在 `README`/`README_EN` 提供本地安装说明。
- 为 `fetch_*_data_with_fallback` 的多源优先级与熔断跳过逻辑新增单元测试（mock provider，避免网络依赖）。
- 新增 GitHub Actions 工作流：自动运行 `python -m unittest discover`。
- 增强 Sina 相关网络请求的鲁棒性：`User-Agent` 随机轮换 + 重试间隔抖动（jitter，避免同频请求）。
- 缓存读取工具新增命中率/部分命中等诊断日志（便于定位“为何读不到缓存”）。

### 下一步（建议优先级）
1. 扩展 provider 级别的可观测性：在 unified入口返回中明确 `provider`、`fallback_route`、`attempt_counts`。
2. 为剩余 HTTP 直连 provider 统一加入“指数退避 + jitter + UA 轮换”（减少重复指纹与拥塞）。
3. 增加更多离线可测用例：缓存合并/partial 命中、异常字段兼容（DTO 规范）。
4. 若对外开放更多 market/资产类型，补齐 DTO 校验模型（pydantic）与 schema 文档。
