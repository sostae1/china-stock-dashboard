# openclaw-data-china-stock

`openclaw-data-china-stock` is an open-source, free-to-use OpenClaw/ClawHub data collection plugin for retail investors. It provides a unified set of `tool_*` interfaces to fetch A-share index, ETF, stocks, and listed options—covering realtime, historical, and minute-level data, plus option contract lookup. The plugin supports multiple data sources with automatic priority and fallback, and disables disk cache writes by default (`data_cache.enabled=false`) to reduce data pollution risk.

## Installation

You can install it directly inside OpenClaw:

```bash
openclaw plugins install @shaoxing-xie/openclaw-data-china-stock
```

## Local install (optional)
If you want to run/debug tools locally (outside OpenClaw), install dependencies first:

```bash
pip install -r requirements.txt
```

## Quick Start (3 minutes)

1. In OpenClaw plugin settings, ensure `scriptPath` points to this repo’s `tool_runner.py` (or keep the default if your setup already mounts it correctly).
2. In your Agent/Workflow, call `tool_fetch_market_data` as the primary cross-asset unified entry.
3. For more stable/offline scenarios, use `tool_read_market_data` / `tool_read_index_*` / `tool_read_etf_*` / `tool_read_option_*`.

### Tushare Backup Configuration
Some fallback routes may use Tushare: please set the environment variable `TUSHARE_TOKEN` (or configure `tushare.token` in `config.yaml`).

Example calls:

- Index (daily historical):
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

- ETF (5-minute):
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

- Option (Greeks):
```yaml
tools:
  - name: tool_fetch_market_data
    params:
      asset_type: option
      view: greeks
      contract_code: "10010910"
```

## What you get

- Index / ETF / **Stock** / Option market data (realtime, historical, minute, opening, Greeks).
- Option contracts (by underlying).
- Optional capabilities: pre-market/policy/news, sector rotation, limit-up pool, northbound flow, etc.
- Optional local Parquet cache reads.

## Why now

Many retail users don’t lack information—they lack a stable, unified data “bottom layer” with consistent parameters, consistent return shapes, and predictable availability.

This plugin focuses on solving common pain points:

- Multiple data sources with consistent interfaces
- Better stability via provider priority + automatic fallback
- Controlled caching: default to **read-only** disk cache mode
- Unified entry point to reduce “tool switching” overhead

## Target audience

- Retail investors focusing on A-shares / ETFs / listed options
- Users who want a workflow-friendly data foundation for OpenClaw/Agent
- Developers who want consistent tool contracts without dealing with many different data providers

## Core capabilities (for stable use)

- As complete as possible by default across index/ETF/stock/options
- Multi-source provider priority and fallback to reduce single-provider failures
- Unified cross-asset entry: `tool_fetch_market_data`
- Compatibility entries kept for convenience: `tool_fetch_index_data`, `tool_fetch_etf_data`, `tool_fetch_option_data`

## Recommended usage

1. Configure the tool runner path in OpenClaw plugin settings to:
   - `tool_runner.py` in this repository
2. In your Agent/Workflow, call:
   - `tool_fetch_market_data` as the primary cross-asset unified entry
3. For cached/offline scenarios (when enabled), use:
   - `tool_read_market_data` or `tool_read_index_*` / `tool_read_etf_*` / `tool_read_option_*`

## MVP tool categories & interface list (release exposure)

Notes:
- “Available” means the tool is registered in `config/tools_manifest.yaml` (runtime uses `config/tools_manifest.json`).
- “Not in the current MVP tool subset” means the implementation exists but is not exposed in the current published tool subset.

### Cross-asset unified entry (recommended)

- `tool_fetch_market_data` (Available)
  - `asset_type=realtime|historical|minute|opening|greeks|global_spot|iopv_snapshot`

### Compatibility entries (merged three entries)

- `tool_fetch_index_data` (Available)
- `tool_fetch_etf_data` (Available)
- `tool_fetch_option_data` (Available)

### Index (Index)

- `tool_fetch_index_realtime` (Available)
- `tool_fetch_index_historical` (Available)
- `tool_fetch_index_minute` (Available)
- `tool_fetch_index_opening` (Available)
- `tool_fetch_global_index_spot` (Not in the current MVP tool subset)

### ETF (ETF)

- `tool_fetch_etf_realtime` (Available)
- `tool_fetch_etf_historical` (Available)
- `tool_fetch_etf_minute` (Available)
- `tool_fetch_etf_iopv_snapshot` (Available)

### Options (Option)

- `tool_fetch_option_realtime` (Available)
- `tool_fetch_option_greeks` (Available)
- `tool_fetch_option_minute` (Available)

### Futures (Futures)

- `tool_fetch_a50_data` (Available)

### Stocks & aggregation (Stock)

- `tool_fetch_stock_realtime` (Available)
- `tool_fetch_stock_historical` (Available)
- `tool_fetch_stock_minute` (Available)
- `tool_stock_data_fetcher` (Available)
- `tool_stock_monitor` (Available)

### Financials (Financials)

- `tool_fetch_stock_financials` (Available)

### Limit-up / sector heat / dragon-tiger / capital flow / northbound

- `tool_fetch_limit_up_stocks` (Available)
- `tool_sector_heat_score` (Available)
- `tool_write_limit_up_with_sector` (Available)
- `tool_limit_up_daily_flow` (Available)
- `tool_dragon_tiger_list` (Available)
- `tool_capital_flow` (Available)
- `tool_fetch_northbound_flow` (Available)
- `tool_fetch_sector_data` (Available)

### Pre-market / policy / macro / announcements / industry news

- `tool_fetch_policy_news` (Available)
- `tool_fetch_macro_commodities` (Available)
- `tool_fetch_overnight_futures_digest` (Available)
- `tool_conditional_overnight_futures_digest` (Available)
- `tool_fetch_announcement_digest` (Available)
- `tool_fetch_industry_news_brief` (Not in the current MVP tool subset)

### Trading sessions / contracts / tradability (Utils)

- `tool_get_option_contracts` (Available)
- `tool_check_trading_status` (Available)
- `tool_get_a_share_market_regime` (Available)
- `tool_filter_a_share_tradability` (Available)
- `tool_fetch_multiple_etf_realtime` (Not in the current MVP tool subset)
- `tool_fetch_multiple_index_realtime` (Not in the current MVP tool subset)
- `tool_fetch_multiple_option_realtime` (Not in the current MVP tool subset)
- `tool_fetch_multiple_option_greeks` (Not in the current MVP tool subset)

### Local cache reads (read_*)

- `tool_read_market_data` (Available)
- `tool_read_index_daily` (Available)
- `tool_read_index_minute` (Available)
- `tool_read_etf_daily` (Available)
- `tool_read_etf_minute` (Available)
- `tool_read_option_minute` (Available)
- `tool_read_option_greeks` (Available)

### Tick (optional, not in MVP)

- `fetch_tick_with_quality` (Not exposed as tool_*)

## Cache policy (important)

### Disk cache semantics

This plugin is designed so that **disk parquet writes are disabled by default**.

In `config.yaml`:

- `data_cache.enabled: false` (default)
  - Disk cache reads are allowed (if existing parquet exists).
  - Disk cache writes are skipped (the plugin will not create/overwrite parquet files).
  - If a cached parquet is unreadable/corrupted, the plugin avoids deleting it in this mode.
- `data_cache.enabled: true`
  - Disk cache reads and writes are both enabled.

## Common tool return contract (recommended fields)

Most `tool_*` functions return a JSON object that typically includes:

- `success`: `true|false`
- `data`: fetched/processed payload (may be `null` on failure)
- `message`: human-readable status/error message
- `source`: where the data came from (e.g., provider name or `cache`)

Some tools additionally provide (when available):

- `count`: number of records/contracts
- `missing_dates`: dates that were not found in cache (used by cache-read tools)
- `timestamp`: data timestamp / query time (string)
- `cache_hit`: whether cache was used (`true|false`)
- `cache_hit_detail`: extra cache hit diagnostics (e.g., which dates/partitions were hit)

### Provider fallback and retries (from `config.yaml`)

The plugin follows the provider priority order in `data_sources.*.priority` (for example, `sina -> eastmoney`), and retries according to:

- Circuit breaker (`data_sources.circuit_breaker`)
  - `enabled`: whether it is enabled
  - `error_threshold`: consecutive error threshold (default `3`)
  - `cooldown_seconds`: cooldown after tripping (default `300`)
- Per-provider retry settings (e.g., `data_sources.etf_minute.eastmoney/sina`)
  - `enabled`: whether the provider is enabled
  - `max_retries`: maximum retry count
  - `retry_delay`: delay between retries (seconds)

## Disclaimer

This plugin is for data collection and engineering practice only. It does not constitute investment advice or a promise of any results. Users are responsible for any risks and outcomes arising from their usage.

## License

MIT License (open-source and free-to-use).
