# openclaw-data-china-stock

开源免费使用的 OpenClaw/ClawHub 插件：面向 A 股、ETF、个股与挂牌期权的数据采集（抓取 + 可选缓存读取）。

## 安装

在 OpenClaw 中直接安装：

```bash
openclaw plugins install @shaoxing-xie/openclaw-data-china-stock
```

## 本地安装（可选）
如果你希望在本地（非 OpenClaw 内）复现/调试工具调用，可以先安装依赖：

```bash
pip install -r requirements.txt
```

## 快速开始（3 分钟）

1. 在 OpenClaw 的插件配置中确认工具执行器路径 `scriptPath` 指向本仓库的 `tool_runner.py`（通常可保持默认）。
2. 在 Agent/Workflow 中优先调用统一入口工具：`tool_fetch_market_data`。
3. 若你在离线/降配网络场景希望尽量稳定：使用 `tool_read_market_data`（或 `tool_read_index_* / tool_read_etf_* / tool_read_option_*`）。

### Tushare 备份配置
部分数据源会以 Tushare 作为可选兜底：请设置环境变量 `TUSHARE_TOKEN`（或在 `config.yaml` 的 `tushare.token` 中配置）。

### 典型调用示例

- 指数（日线历史）：
```yaml
tools:
  - name: tool_fetch_market_data
    params:
      asset_type: index
      view: historical
      asset_code: "000001"
      period: daily
      start_date: "20260201"
      end_date: "20260228"
```

- ETF（5 分钟分钟线）：
```yaml
tools:
  - name: tool_fetch_market_data
    params:
      asset_type: etf
      view: minute
      asset_code: "510300"
      period: "5"
      start_date: "20260201"
      end_date: "20260228"
```

- 期权（Greeks）：
```yaml
tools:
  - name: tool_fetch_market_data
    params:
      asset_type: option
      view: greeks
      contract_code: "10010910"
```

## 缓存策略（默认语义）

默认 `data_cache.enabled=false` 时：插件会**允许读取已有磁盘 parquet 缓存**，但**跳过磁盘 parquet 写入**（避免生成/覆盖文件带来的污染风险）。

## 你能获得什么

本插件包含 `data_collection` 与 `merged` 的工具实现，并对外提供稳定的 `tool_*` 接口，覆盖：

- 指数 / ETF / 个股 / 期权市场数据（实时 realtime、历史 historical、分钟 minute、开盘 opening / Greeks）。
- 期权合约列表（按 underlying）。
- 可选能力：盘前/政策/news、行业轮动、涨停池、北向资金、以及更多。
- 可选：本地 Parquet 缓存读取。

本插件的目标是让散户用户能“少踩坑”：通过多数据源自动优先级与降级（例如 AkShare / Sina / 东方财富 / 可选 Tushare），降低单一接口不可用导致的中断；并提供统一入口，便于直接在 OpenClaw/Agent 工作流里稳定调用。

## 为什么现在需要它（Why now）

很多散户并不是“缺信息”，而是缺少一套**统一口径、稳定可用**的数据底座：

* 多个数据源各自为政：接口字段/格式不一致，难以直接用于盘中流程
* 单一源不稳定：限流/不可用会直接导致流程中断
* 缓存与口径难控：数据可能“看起来有”，但难以判断是否来自同一来源、同一策略

本插件把这些问题收敛成可复用的 `tool_*` 接口：统一参数、统一返回契约、并内置多数据源优先级与降级。

## 适用人群

* 以 A 股/ETF/期权为主要研究对象的散户投资者
* 希望把“盯盘 + 获取数据 + 生成结论”流程化、自动化的个人用户
* 想基于公开工具二次开发、但不愿为数据口径折腾太多的开发者

## 核心能力（面向“稳定可用”）

* 默认尽可能全面：指数、ETF、个股、挂牌期权的实时/历史/分钟级数据与期权合约查询
* 多数据源更稳定：provider 优先级 + 自动降级（减少因单接口失败导致的中断）
* 统一接口更易用：首发推荐 `tool_fetch_market_data` 跨资产统一入口，并保留兼容入口
* 缓存策略更可控：默认关闭磁盘缓存写入（`data_cache.enabled=false`），减少不必要的数据污染

## 快速开始（最短 3 步）

1. 在 OpenClaw 插件设置中配置工具执行器路径为：`tool_runner.py`（本仓库内）
2. 在 Agent/Workflow 中优先调用：`tool_fetch_market_data`（例如 `asset_type=index` + `view=historical`）
3. 如需离线/离散补数：再按需开启 `data_cache.enabled=true`（磁盘缓存写入）

## 免责声明

本插件仅用于数据采集与工程实践，不构成任何投资建议或收益承诺。任何使用行为与由此产生的风险后果由使用者自行承担。

## 推荐的使用方式

1. 在 OpenClaw 插件设置中配置工具执行器路径为：
   - `tool_runner.py`（本仓库内）
2. 在你的 Agent/Workflow 里，优先调用：
   - `tool_fetch_market_data`（跨资产统一入口）
3. 如果你希望使用缓存/离线场景（当启用缓存时）：
   - `tool_read_market_data` 或 `tool_read_index_*` / `tool_read_etf_*` / `tool_read_option_*`

## 背景与用途

`openclaw-data-china-stock` 是一个面向 A 股市场的数据采集插件：围绕指数、ETF、个股、挂牌期权等常用资产的数据抓取与（可选的）Parquet 本地缓存读取，为 OpenClaw 的工作流/Agent 提供统一的 `tool_*` 接口。

本插件默认不写入磁盘缓存（通过 `config.yaml` 中的 `data_cache.enabled` 控制），更适合“在线抓取优先 + 已有缓存可读”的使用方式。

## 面向散户的痛点与解决方案

- 默认尽可能全面：覆盖指数、ETF、A股个股、以及挂牌期权的实时/历史/分钟级行情与期权合约查询。
- 多数据源更稳定：内置 provider 优先级与自动降级；当某些接口限流/不可用时，优先尝试下一个数据源。
- 更“可控”的缓存策略：默认关闭磁盘缓存写入（`data_cache.enabled=false`），避免生成/覆盖大量数据文件；你可以在需要时显式开启写入。
- 统一接口更易用：首发推荐 `tool_fetch_market_data` 跨资产统一入口，兼容接口保留 `tool_fetch_index_data/tool_fetch_etf_data/tool_fetch_option_data`，减少“换工具/换参数”的学习成本。

## 工具分类与接口清单（用于讨论首发暴露范围）

说明：以下 `tool_id` 来自本仓库 `config/tools_manifest.yaml`（运行时以 `config/tools_manifest.json` 注册为准）。
其中“已注册”表示当前版本已在 OpenClaw 中暴露，直接可被工具调用；“未纳入当前首发清单”表示在本仓库代码中可能存在，但尚未纳入当前工具清单（需要后续版本补齐）。

### 跨资产统一入口（推荐）

- `tool_fetch_market_data`（已注册）
  - `asset_type=realtime|historical|minute|opening|greeks|global_spot|iopv_snapshot` 维度统一入口（配合 `asset_code/contract_code` 等参数）

### 兼容入口（merged 三入口）

- `tool_fetch_index_data`（已注册）
- `tool_fetch_etf_data`（已注册）
- `tool_fetch_option_data`（已注册）

### 指数数据（Index）

- `tool_fetch_index_realtime`（已注册）
- `tool_fetch_index_historical`（已注册）
- `tool_fetch_index_minute`（已注册）
- `tool_fetch_index_opening`（已注册）
- `tool_fetch_global_index_spot`（未纳入当前首发清单）

### ETF 数据（ETF）

- `tool_fetch_etf_realtime`（已注册）
- `tool_fetch_etf_historical`（已注册）
- `tool_fetch_etf_minute`（已注册）
- `tool_fetch_etf_iopv_snapshot`（已注册）

### 期权数据（Option）

- `tool_fetch_option_realtime`（已注册）
- `tool_fetch_option_greeks`（已注册）
- `tool_fetch_option_minute`（已注册）

### 期指/期货（Futures）

- `tool_fetch_a50_data`（已注册）

### 个股与聚合（Stock）

- `tool_fetch_stock_realtime`（已注册）
- `tool_fetch_stock_historical`（已注册）
- `tool_fetch_stock_minute`（已注册）
- `tool_stock_data_fetcher`（已注册）
- `tool_stock_monitor`（已注册）

### 财务指标（Financials）

- `tool_fetch_stock_financials`（已注册）

### 涨停/板块/龙虎榜/资金流/北向

- `tool_fetch_limit_up_stocks`（已注册）
- `tool_sector_heat_score`（已注册）
- `tool_write_limit_up_with_sector`（已注册；是否写入需看缓存策略/配置）
- `tool_limit_up_daily_flow`（已注册；是否写入需看缓存策略/配置）
- `tool_dragon_tiger_list`（已注册）
- `tool_capital_flow`（已注册）
- `tool_fetch_northbound_flow`（已注册）
- `tool_fetch_sector_data`（已注册）

### 盘前/政策/宏观/公告/行业要闻

- `tool_fetch_policy_news`（已注册）
- `tool_fetch_macro_commodities`（已注册）
- `tool_fetch_overnight_futures_digest`（已注册）
- `tool_conditional_overnight_futures_digest`（已注册）
- `tool_fetch_announcement_digest`（已注册）
- `tool_fetch_industry_news_brief`（未纳入当前首发清单）

### 交易时段/合约/可交易性工具（Utils）

- `tool_get_option_contracts`（已注册）
- `tool_check_trading_status`（已注册）
- `tool_get_a_share_market_regime`（已注册）
- `tool_filter_a_share_tradability`（已注册）
- `tool_fetch_multiple_etf_realtime`（未纳入当前首发清单）
- `tool_fetch_multiple_index_realtime`（未纳入当前首发清单）
- `tool_fetch_multiple_option_realtime`（未纳入当前首发清单）
- `tool_fetch_multiple_option_greeks`（未纳入当前首发清单）

### 本地缓存读取（read_*）

- `tool_read_market_data`（已注册）
- `tool_read_index_daily`（已注册）
- `tool_read_index_minute`（已注册）
- `tool_read_etf_daily`（已注册）
- `tool_read_etf_minute`（已注册）
- `tool_read_option_minute`（已注册）
- `tool_read_option_greeks`（已注册）

### Tick（可选，不纳入首发）

- `fetch_tick_with_quality`（未纳入当前首发清单）

## 首发 MVP 工具（建议优先用）

- `tool_fetch_market_data`
  - 跨资产统一入口（推荐）
- `tool_get_option_contracts`
  - 根据 underlying 获取期权合约
- 兼容入口（merged 三入口）：
  - `tool_fetch_index_data`
  - `tool_fetch_etf_data`
  - `tool_fetch_option_data`

## 缓存策略（重要）

### 磁盘缓存语义（Disk Parquet）

插件默认设计为：**不开磁盘 parquet 写入**。

在 `config.yaml`：

- `data_cache.enabled: false`（默认）
  - 允许磁盘缓存“读取”（如果已有 parquet 存在）
  - 跳过磁盘缓存“写入”（插件不会创建/覆盖 parquet 文件）
  - 如果缓存 parquet 不可读/损坏，本模式会避免删除坏文件
- `data_cache.enabled: true`
  - 允许磁盘缓存“读 + 写”

### 通用工具返回契约（建议字段）

大多数 `tool_*` 会返回 JSON 对象，常见字段包括：

- `success`: `true|false`
- `data`: 获取/处理后的数据（失败时可能是 `null`）
- `message`: 可读的状态/错误信息
- `source`: 数据来源（例如 provider 名称或 `cache`）

部分工具还会返回额外字段，如：

- `count`: 记录数/合约数
- `missing_dates`: 缓存中未找到的日期（给 `read_*` 类工具使用）

当可用时，部分工具可能还会提供：

- `timestamp`: 数据时间戳/查询时间（字符串）
- `cache_hit`: 是否命中缓存（`true|false`）
- `cache_hit_detail`: 缓存命中详情（例如命中了哪些日期/分区）

### Provider fallback 与重试（来自 `config.yaml`）

插件会按 `data_sources.*.priority` 的顺序尝试数据源（例如 `sina -> eastmoney` 等），失败后按以下规则重试：

- 熔断（circuit breaker）：`data_sources.circuit_breaker`
  - `enabled`：是否启用
  - `error_threshold`：连续错误阈值（默认 `3`）
  - `cooldown_seconds`：熔断后冷却时间（默认 `300`）
- 重试（per provider）：例如 `data_sources.etf_minute.eastmoney/sina`
  - `enabled`：是否启用该 provider
  - `max_retries`：最大重试次数
  - `retry_delay`：每次重试的延迟（秒）

## License

MIT License（开源免费使用）。

