---
name: openclaw-data-china-stock
description: A-share/ETF/Option market data collection plugin for OpenClaw.
tags: [data, china, etf, option, openclaw]
---

# OpenClaw Data China Stock

This plugin provides a ClawHub/OpenClaw compatible toolset for collecting A-share, ETF, and listed option data.

## Key tools

- `tool_fetch_market_data`: Cross-asset unified entry (recommended).
- `tool_fetch_index_data` / `tool_fetch_etf_data` / `tool_fetch_option_data`: Compatibility/alias unified entries.
- `tool_get_option_contracts`: Fetch option contracts by underlying.
- `tool_read_market_data` / `tool_read_*`: Read previously cached Parquet data (when enabled).

## Safety and independence

The plugin is designed to run independently (no dependency on any other repository):

- It does not inject `~/.openclaw/.env`.
- It supports plugin-specific cache/data paths (configured inside the plugin).

## Why it helps retail users

- Unified cross-asset entry (`tool_fetch_market_data`) to reduce “tool switching” friction.
- Multi-source provider priority + automatic fallback to avoid single-provider outages breaking your workflow.
- Default read-only disk cache semantics (`data_cache.enabled=false`) to minimize local data pollution risk.

## Typical usage

Example: fetch A-share index daily historical data:

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

Example: fetch ETF 5-minute bars:

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

## Cache policy

- Default: `data_cache.enabled=false`
  - Disk parquet reads are allowed (if cache exists).
  - Disk parquet writes are skipped.
  - Corrupted parquet files are not deleted in this mode.
- `data_cache.enabled=true`
  - Disk parquet reads and writes are both enabled.

