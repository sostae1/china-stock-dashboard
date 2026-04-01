"""
A 股扩展基本面与市场结构工具：证券主数据、三大表、公司行为、两融、大宗。

返回约定（与 ROADMAP 一致）：success / message / data / source / provider /
fallback_route / attempt_counts；data 多为 records 列表（JSON 友好）。
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from plugins.data_collection.utils.provider_preference import reorder_provider_chain

logger = logging.getLogger(__name__)

try:
    import akshare as ak

    AKSHARE_AVAILABLE = True
except Exception:  # noqa: BLE001
    AKSHARE_AVAILABLE = False
    ak = None  # type: ignore[assignment]


def _obs(
    source: str,
    *,
    route: Optional[List[str]] = None,
    attempts: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    r = route or [source]
    return {
        "source": source,
        "provider": source,
        "fallback_route": r,
        "attempt_counts": attempts or {source: 1},
    }


def _ok_payload(
    data: Any,
    *,
    source: str,
    route: Optional[List[str]] = None,
    attempts: Optional[Dict[str, int]] = None,
    count: Optional[int] = None,
) -> Dict[str, Any]:
    meta = _obs(source, route=route, attempts=attempts)
    out: Dict[str, Any] = {
        "success": True,
        "message": "ok",
        "data": data,
        **meta,
    }
    if count is not None:
        out["count"] = count
    return out


def _err_payload(msg: str, *, source: str = "none") -> Dict[str, Any]:
    return {
        "success": False,
        "message": msg,
        "data": None,
        **_obs(source),
        "count": 0,
    }


def _norm_code_6(code: str) -> str:
    c = (code or "").strip()
    if c.upper().endswith((".SH", ".SZ", ".BJ")):
        c = c.split(".")[0]
    if len(c) > 2 and c.lower()[:2] in ("sh", "sz", "bj") and c[2:].isdigit():
        c = c[2:]
    return c


def _to_em_prefixed(code: str) -> str:
    """东财部分接口使用 SH600519 / SZ000001 / BJ920000。"""
    c = _norm_code_6(code)
    if len(c) != 6 or not c.isdigit():
        return code.strip().upper()
    if c.startswith(("5", "6", "9")):
        return f"SH{c}"
    if c.startswith(("0", "1", "2", "3")):
        return f"SZ{c}"
    if c.startswith(("4", "8")):
        return f"BJ{c}"
    return f"SH{c}"


def _to_sina_stock(code: str) -> str:
    """新浪财报等接口使用 sh600000 / sz000001。"""
    c = _norm_code_6(code)
    if len(c) != 6 or not c.isdigit():
        return (code or "").strip().lower()
    if c.startswith(("5", "6", "9")):
        return f"sh{c}"
    if c.startswith(("0", "1", "2", "3")):
        return f"sz{c}"
    if c.startswith(("4", "8")):
        return f"bj{c}"
    return f"sh{c}"


def _df_records(df: Optional[pd.DataFrame], max_rows: int = 0) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    if max_rows and max_rows > 0:
        df = df.head(max_rows)
    return df.to_dict(orient="records")


def tool_fetch_a_share_universe(
    max_rows: int = 0,
    provider_preference: str = "auto",
) -> Dict[str, Any]:
    """
    沪深京 A 股代码与简称（主数据）。优先 AkShare stock_info_a_code_name，失败则尝试东财全市场快照截断。
    provider_preference= eastmoney 时先尝试东财快照再代码表。
    """
    if not AKSHARE_AVAILABLE:
        return _err_payload("AkShare 未安装")

    attempts: Dict[str, int] = {}

    def route_code_name() -> Tuple[List[Dict[str, Any]], List[str]]:
        attempts["stock_info_a_code_name"] = attempts.get("stock_info_a_code_name", 0) + 1
        df = ak.stock_info_a_code_name()  # type: ignore[union-attr]
        return _df_records(df, max_rows), ["stock_info_a_code_name"]

    def route_spot_em() -> Tuple[List[Dict[str, Any]], List[str]]:
        attempts["stock_zh_a_spot_em"] = attempts.get("stock_zh_a_spot_em", 0) + 1
        df = ak.stock_zh_a_spot_em()  # type: ignore[union-attr]
        use = df
        if max_rows and max_rows > 0:
            use = df.head(max_rows)
        cols = [c for c in use.columns if c in ("代码", "名称", "最新价", "涨跌幅", "成交额", "总市值")]
        if not cols:
            cols = list(use.columns)[:12]
        recs = use[cols].to_dict(orient="records")
        return recs, ["stock_zh_a_spot_em"]

    tagged: List[Tuple[str, str, Callable[[], Tuple[List[Dict[str, Any]], List[str]]]]] = [
        ("standard", "stock_info_a_code_name", route_code_name),
        ("eastmoney", "stock_zh_a_spot_em", route_spot_em),
    ]
    tagged = reorder_provider_chain(provider_preference, tagged)

    all_routes: List[str] = []
    for _tag, _label, fn in tagged:
        try:
            recs, rts = fn()
            all_routes.extend(rts)
            if recs:
                return _ok_payload(
                    recs,
                    source="akshare",
                    route=all_routes,
                    attempts=dict(attempts),
                    count=len(recs),
                )
        except Exception as e:  # noqa: BLE001
            logger.debug("universe route failed: %s", e)

    return _err_payload("无法获取 A 股列表（AkShare 代码表与快照均失败）")


def tool_fetch_stock_financial_reports(
    stock_code: str,
    statement_type: str = "balance",
    provider_preference: str = "auto",
) -> Dict[str, Any]:
    """
    按报告期的财务报表（资产负债表 / 利润表 / 现金流量表）。

    statement_type: balance | income | cashflow
    数据源：东财报告期表优先；可选新浪财报 stock_financial_report_sina 降级。
    provider_preference=sina 时优先新浪。
    """
    if not AKSHARE_AVAILABLE:
        return _err_payload("AkShare 未安装")

    st = (statement_type or "balance").strip().lower()
    sym = _to_em_prefixed(stock_code)
    fn_map = {
        "balance": ("stock_balance_sheet_by_report_em", ak.stock_balance_sheet_by_report_em),
        "income": ("stock_profit_sheet_by_report_em", ak.stock_profit_sheet_by_report_em),
        "cashflow": ("stock_cash_flow_sheet_by_report_em", ak.stock_cash_flow_sheet_by_report_em),
    }
    if st not in fn_map:
        return _err_payload(f"不支持的 statement_type={statement_type}，请用 balance|income|cashflow")

    em_name, em_fn = fn_map[st]
    sheet_cn = {"balance": "资产负债表", "income": "利润表", "cashflow": "现金流量表"}[st]
    sina_stock = _to_sina_stock(stock_code)

    def try_em() -> pd.DataFrame:
        return em_fn(symbol=sym)  # type: ignore[misc]

    def try_sina() -> pd.DataFrame:
        return ak.stock_financial_report_sina(stock=sina_stock, symbol=sheet_cn)  # type: ignore[union-attr]

    tagged_df: List[Tuple[str, str, Callable[[], pd.DataFrame]]] = [
        ("eastmoney", em_name, try_em),
        ("sina", "stock_financial_report_sina", try_sina),
    ]
    tagged_df = reorder_provider_chain(provider_preference, tagged_df)

    attempts: Dict[str, int] = {}
    routes: List[str] = []
    last_err: Optional[str] = None
    for _tag, name, fn in tagged_df:
        try:
            attempts[name] = attempts.get(name, 0) + 1
            df = fn()
            routes.append(name)
            recs = _df_records(df, 0)
            return _ok_payload(
                recs,
                source="akshare",
                route=routes,
                attempts=attempts,
                count=len(recs),
            )
        except Exception as e:  # noqa: BLE001
            last_err = f"{name} 失败: {e}"
            logger.debug("%s", last_err)
    return _err_payload(last_err or "财务报表获取失败")


def tool_fetch_stock_corporate_actions(
    action_kind: str,
    stock_code: str = "",
    start_date: str = "",
    end_date: str = "",
) -> Dict[str, Any]:
    """
    公司行为统一入口。

    action_kind:
      - dividend: 分红派息（需 stock_code）
      - restricted_unlock: 限售解禁队列（需 stock_code）
      - issuance: 增发管道（stock_code 空则全市场；否则按代码过滤）
      - allotment: 配股（需 stock_code，可选 start_date/end_date）
      - buyback: 回购（stock_code 空则全市场；否则按代码过滤）
    """
    if not AKSHARE_AVAILABLE:
        return _err_payload("AkShare 未安装")

    kind = (action_kind or "").strip().lower()
    code6 = _norm_code_6(stock_code) if stock_code else ""

    try:
        if kind == "dividend":
            if not code6:
                return _err_payload("dividend 需要 stock_code")
            df = ak.stock_dividend_cninfo(symbol=code6)  # type: ignore[union-attr]
            recs = _df_records(df, 0)
            return _ok_payload(recs, source="akshare", route=["stock_dividend_cninfo"], attempts={"stock_dividend_cninfo": 1}, count=len(recs))

        if kind == "restricted_unlock":
            if not code6:
                return _err_payload("restricted_unlock 需要 stock_code")
            df = ak.stock_restricted_release_queue_em(symbol=code6)  # type: ignore[union-attr]
            recs = _df_records(df, 0)
            return _ok_payload(
                recs,
                source="akshare",
                route=["stock_restricted_release_queue_em"],
                attempts={"stock_restricted_release_queue_em": 1},
                count=len(recs),
            )

        if kind == "allotment":
            if not code6:
                return _err_payload("allotment 需要 stock_code")
            sd = start_date or "19700101"
            ed = end_date or "22220222"
            df = ak.stock_allotment_cninfo(symbol=code6, start_date=sd, end_date=ed)  # type: ignore[union-attr]
            recs = _df_records(df, 0)
            return _ok_payload(recs, source="akshare", route=["stock_allotment_cninfo"], attempts={"stock_allotment_cninfo": 1}, count=len(recs))

        if kind == "issuance":
            df = ak.stock_qbzf_em()  # type: ignore[union-attr]
            if code6 and "股票代码" in df.columns:
                df = df[df["股票代码"].astype(str).str.strip() == code6]
            recs = _df_records(df, 0)
            return _ok_payload(recs, source="akshare", route=["stock_qbzf_em"], attempts={"stock_qbzf_em": 1}, count=len(recs))

        if kind == "buyback":
            df = ak.stock_repurchase_em()  # type: ignore[union-attr]
            if code6 and "股票代码" in df.columns:
                df = df[df["股票代码"].astype(str).str.strip() == code6]
            recs = _df_records(df, 0)
            return _ok_payload(recs, source="akshare", route=["stock_repurchase_em"], attempts={"stock_repurchase_em": 1}, count=len(recs))

        return _err_payload(f"不支持的 action_kind={action_kind}")
    except Exception as e:  # noqa: BLE001
        return _err_payload(f"公司行为查询失败: {e}")


def tool_fetch_margin_trading(
    market: str = "sh",
    data_kind: str = "summary",
    date: str = "",
    start_date: str = "",
    end_date: str = "",
) -> Dict[str, Any]:
    """
    融资融券数据。

    market: sh | sz
    data_kind:
      - summary: 沪市 stock_margin_sse(start,end) 或 深市 stock_margin_szse(date)
      - detail: 两市明细 stock_margin_detail_sse / stock_margin_detail_szse（需要 date）
      - underlying_sz: 深市标的与保证金比例 stock_margin_underlying_info_szse（无日期）
    """
    if not AKSHARE_AVAILABLE:
        return _err_payload("AkShare 未安装")

    m = (market or "sh").strip().lower()
    dk = (data_kind or "summary").strip().lower()
    attempts: Dict[str, int] = {}
    routes: List[str] = []

    try:
        if dk == "underlying_sz":
            if m != "sz":
                return _err_payload("underlying_sz 仅支持 market=sz")
            attempts["stock_margin_underlying_info_szse"] = 1
            df = ak.stock_margin_underlying_info_szse()  # type: ignore[union-attr]
            routes.append("stock_margin_underlying_info_szse")
            recs = _df_records(df, 0)
            return _ok_payload(recs, source="akshare", route=routes, attempts=attempts, count=len(recs))

        if dk == "detail":
            d = (date or "").strip()
            if len(d) != 8 or not d.isdigit():
                return _err_payload("detail 需要有效 date=YYYYMMDD")
            if m == "sh":
                attempts["stock_margin_detail_sse"] = 1
                df = ak.stock_margin_detail_sse(date=d)  # type: ignore[union-attr]
                routes.append("stock_margin_detail_sse")
            else:
                attempts["stock_margin_detail_szse"] = 1
                df = ak.stock_margin_detail_szse(date=d)  # type: ignore[union-attr]
                routes.append("stock_margin_detail_szse")
            recs = _df_records(df, 0)
            return _ok_payload(recs, source="akshare", route=routes, attempts=attempts, count=len(recs))

        if dk == "summary":
            if m == "sh":
                sd = start_date or "20010106"
                ed = end_date or date or "20991231"
                attempts["stock_margin_sse"] = 1
                df = ak.stock_margin_sse(start_date=sd, end_date=ed)  # type: ignore[union-attr]
                routes.append("stock_margin_sse")
            else:
                d = date or end_date or start_date
                if not d or len(d) != 8:
                    return _err_payload("深市 summary 需要 date=YYYYMMDD")
                attempts["stock_margin_szse"] = 1
                df = ak.stock_margin_szse(date=d)  # type: ignore[union-attr]
                routes.append("stock_margin_szse")
            recs = _df_records(df, 0)
            return _ok_payload(recs, source="akshare", route=routes, attempts=attempts, count=len(recs))

        return _err_payload(f"不支持的 data_kind={data_kind}")
    except Exception as e:  # noqa: BLE001
        return _err_payload(f"融资融券查询失败: {e}")


def tool_fetch_block_trades(
    block_kind: str = "mrtj",
    start_date: str = "",
    end_date: str = "",
    window: str = "近三月",
) -> Dict[str, Any]:
    """
    大宗交易（东财）。

    block_kind:
      - sctj: 市场统计 stock_dzjy_sctj
      - mrtj: 每日统计 stock_dzjy_mrtj
      - mrmx: 每日明细 A 股 stock_dzjy_mrmx
      - hygtj: 活跃 A 股统计 stock_dzjy_hygtj（window 如 近三月）
      - yybph: 营业部排行 stock_dzjy_yybph
    """
    if not AKSHARE_AVAILABLE:
        return _err_payload("AkShare 未安装")

    bk = (block_kind or "mrtj").strip().lower()
    attempts: Dict[str, int] = {}
    routes: List[str] = []

    try:
        if bk == "sctj":
            attempts["stock_dzjy_sctj"] = 1
            df = ak.stock_dzjy_sctj()  # type: ignore[union-attr]
            routes.append("stock_dzjy_sctj")
        elif bk == "mrtj":
            sd = start_date or "20240105"
            ed = end_date or sd
            attempts["stock_dzjy_mrtj"] = 1
            df = ak.stock_dzjy_mrtj(start_date=sd, end_date=ed)  # type: ignore[union-attr]
            routes.append("stock_dzjy_mrtj")
        elif bk == "mrmx":
            sd = start_date or "20240105"
            ed = end_date or sd
            attempts["stock_dzjy_mrmx"] = 1
            df = ak.stock_dzjy_mrmx(symbol="A股", start_date=sd, end_date=ed)  # type: ignore[union-attr]
            routes.append("stock_dzjy_mrmx")
        elif bk == "hygtj":
            attempts["stock_dzjy_hygtj"] = 1
            df = ak.stock_dzjy_hygtj(symbol=window or "近三月")  # type: ignore[union-attr]
            routes.append("stock_dzjy_hygtj")
        elif bk == "yybph":
            attempts["stock_dzjy_yybph"] = 1
            df = ak.stock_dzjy_yybph(symbol=window or "近三月")  # type: ignore[union-attr]
            routes.append("stock_dzjy_yybph")
        else:
            return _err_payload(f"不支持的 block_kind={block_kind}")

        recs = _df_records(df, 0)
        return _ok_payload(recs, source="akshare", route=routes, attempts=attempts, count=len(recs))
    except Exception as e:  # noqa: BLE001
        return _err_payload(f"大宗交易查询失败: {e}")
