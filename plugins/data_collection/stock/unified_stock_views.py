"""
供 tool_fetch_market_data（stock + 扩展 view）使用的分时/盘前/市场总貌等视图。
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List

from plugins.data_collection.stock.fundamentals_extended import (
    _df_records,
    _err_payload,
    _norm_code_6,
    _ok_payload,
)

logger = logging.getLogger(__name__)

try:
    import akshare as ak

    AKSHARE_AVAILABLE = True
except Exception:  # noqa: BLE001
    AKSHARE_AVAILABLE = False
    ak = None  # type: ignore[assignment]


def _to_sina_symbol(code: str) -> str:
    c = _norm_code_6(code)
    if c.startswith(("5", "6", "9")):
        return f"sh{c}"
    return f"sz{c}"


def fetch_stock_timeshare_view(stock_code: str) -> Dict[str, Any]:
    """当日分时：东财 intraday 优先，新浪兜底。"""
    if not AKSHARE_AVAILABLE:
        return _err_payload("AkShare 未安装")

    sym6 = _norm_code_6(stock_code)
    routes: List[str] = []
    attempts: Dict[str, int] = {}

    try:
        attempts["stock_intraday_em"] = attempts.get("stock_intraday_em", 0) + 1
        df = ak.stock_intraday_em(symbol=sym6)  # type: ignore[union-attr]
        routes.append("stock_intraday_em")
        recs = _df_records(df, 0)
        return _ok_payload(recs, source="akshare", route=routes, attempts=attempts, count=len(recs))
    except Exception as e:  # noqa: BLE001
        logger.debug("stock_intraday_em failed: %s", e)

    try:
        today = datetime.now().strftime("%Y%m%d")
        attempts["stock_intraday_sina"] = attempts.get("stock_intraday_sina", 0) + 1
        df = ak.stock_intraday_sina(symbol=_to_sina_symbol(sym6), date=today)  # type: ignore[union-attr]
        routes.append("stock_intraday_sina")
        recs = _df_records(df, 0)
        return _ok_payload(recs, source="akshare", route=routes, attempts=attempts, count=len(recs))
    except Exception as e:  # noqa: BLE001
        return _err_payload(f"分时数据不可用: {e}")


def fetch_stock_pre_market_view(stock_code: str) -> Dict[str, Any]:
    """盘前参考：东财盘前分钟。"""
    if not AKSHARE_AVAILABLE:
        return _err_payload("AkShare 未安装")

    sym6 = _norm_code_6(stock_code)
    try:
        df = ak.stock_zh_a_hist_pre_min_em(  # type: ignore[union-attr]
            symbol=sym6,
            start_time="09:00:00",
            end_time="09:30:00",
        )
        recs = _df_records(df, 0)
        return _ok_payload(
            recs,
            source="akshare",
            route=["stock_zh_a_hist_pre_min_em"],
            attempts={"stock_zh_a_hist_pre_min_em": 1},
            count=len(recs),
        )
    except Exception as e:  # noqa: BLE001
        return _err_payload(f"盘前数据不可用: {e}")


def fetch_stock_market_overview(trade_date: str = "") -> Dict[str, Any]:
    """上交所市场总貌 + 深交所市场总貌（单日）。"""
    if not AKSHARE_AVAILABLE:
        return _err_payload("AkShare 未安装")

    out: Dict[str, Any] = {"sse": None, "szse": None}
    routes: List[str] = []
    attempts: Dict[str, int] = {}

    try:
        attempts["stock_sse_summary"] = 1
        d1 = ak.stock_sse_summary()  # type: ignore[union-attr]
        routes.append("stock_sse_summary")
        out["sse"] = _df_records(d1, 0)
    except Exception as e:  # noqa: BLE001
        out["sse_error"] = str(e)

    d = (trade_date or "").strip()
    if not d or len(d) != 8:
        d = datetime.now().strftime("%Y%m%d")
    try:
        attempts["stock_szse_summary"] = 1
        d2 = ak.stock_szse_summary(date=d)  # type: ignore[union-attr]
        routes.append("stock_szse_summary")
        out["szse"] = _df_records(d2, 0)
    except Exception as e:  # noqa: BLE001
        out["szse_error"] = str(e)

    ok = out.get("sse") is not None or out.get("szse") is not None
    if not ok:
        return _err_payload("市场总貌不可用（上交所/深交所摘要均失败）")

    return {
        "success": True,
        "message": "ok",
        "data": out,
        "source": "akshare",
        "provider": "akshare",
        "fallback_route": routes,
        "attempt_counts": attempts,
    }


def fetch_stock_valuation_snapshot_view(stock_code: str) -> Dict[str, Any]:
    """与 tool_fetch_stock_financials 字段对齐的轻量封装（统一入口用）。"""
    from plugins.data_collection.financials import tool_fetch_stock_financials

    sym = _norm_code_6(stock_code)
    res = tool_fetch_stock_financials(symbols=sym, lookback_report_count=1)
    fin = (res.get("financials") or [{}])[0]
    return {
        "success": bool(fin.get("success")),
        "message": "ok" if fin.get("success") else (fin.get("error") or res.get("error") or "无数据"),
        "data": fin,
        "source": "akshare",
        "provider": "financials_em",
        "fallback_route": ["tool_fetch_stock_financials"],
        "attempt_counts": {"stock_financial_analysis_indicator_em": 1},
    }
