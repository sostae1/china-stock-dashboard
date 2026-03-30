"""
Cache read helper expected by `plugins/merged/read_market_data.py`.

The merged tool imports:
  from data_access.read_cache_data import read_cache_data

This repository originally relied on a `plugins/data_access/*` module.
We implement it here as a thin wrapper over `src/data_cache.py`, so
`tool_read_market_data` works in an independent plugin install.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from src.data_cache import get_cache_file_path, load_cached_data, parse_date_range


def _normalize_dates(
    *,
    date: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
) -> List[str]:
    if date:
        return [date]
    if start_date and end_date:
        return parse_date_range(start_date, end_date)
    # Caller usually passes at least one of (date) or (start_date/end_date).
    # We return empty list to produce a clear error message.
    return []


def read_cache_data(
    data_type: str,
    symbol: str,
    period: Optional[str] = None,
    *,
    date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Read cached parquet data for a given data_type and symbol.

    Returns a JSON-serializable structure:
      - success: bool
      - message: str
      - data: list[dict] or None
      - missing_dates: list[str] (when a date range is requested)
    """

    dates = _normalize_dates(date=date, start_date=start_date, end_date=end_date)
    if not dates:
        return {
            "success": False,
            "message": "缺少 date 或 start_date/end_date",
            "data": None,
            "missing_dates": [],
        }

    dfs: List[pd.DataFrame] = []
    missing_dates: List[str] = []

    for d in dates:
        file_path = get_cache_file_path(
            data_type=data_type,
            symbol=symbol,
            date=d,
            period=period,
        )
        df = load_cached_data(file_path)
        if df is None:
            missing_dates.append(d)
            continue
        dfs.append(df)

    if not dfs:
        return {
            "success": False,
            "message": "cache_miss",
            "data": None,
            "missing_dates": missing_dates,
        }

    df_all = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]
    records = df_all.to_dict(orient="records")

    partial = len(missing_dates) > 0
    return {
        "success": True,
        "message": "ok" if not partial else "partial_cache_hit",
        "data": records,
        "missing_dates": missing_dates,
        "cache_hit": True,
    }


__all__ = ["read_cache_data"]

