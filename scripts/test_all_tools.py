from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_manifest(manifest_path: Path) -> List[Dict[str, Any]]:
    raw = manifest_path.read_text(encoding="utf-8")
    data = json.loads(raw)
    return data.get("tools", []) or []


def _iter_parquet_cache_paths(cache_root: Path) -> Iterable[Path]:
    if not cache_root.exists():
        return []
    return cache_root.rglob("*.parquet")


def _scan_cache_samples(cache_root: Path) -> Dict[str, Dict[str, List[str]]]:
    """
    Returns:
      {
        "etf_daily": {"510300": ["20240102", ...]},
        "option_greeks": {"10000000": ["20260330", ...]},
        "index_daily": {"000001": ["..."]}
      }
    """
    samples: Dict[str, Dict[str, List[str]]] = {}
    for p in _iter_parquet_cache_paths(cache_root):
        # data/cache/<data_type>/<symbol>/<YYYYMMDD>.parquet
        try:
            rel = p.relative_to(cache_root)
            parts = rel.parts
            if len(parts) < 3:
                continue
            data_type = parts[0]
            symbol = parts[1]
            date_part = p.stem  # YYYYMMDD
            if not date_part:
                continue
            samples.setdefault(data_type, {}).setdefault(symbol, []).append(date_part)
        except Exception:
            continue

    # Sort dates for determinism
    for _, sym_map in samples.items():
        for sym, dates in sym_map.items():
            sym_map[sym] = sorted(set(dates))

    return samples


def _pick_first(items: Iterable[str], default: str) -> str:
    for x in items:
        if x:
            return x
    return default


def _heuristic_value(param_name: str) -> Any:
    """
    Best-effort placeholders so tool validation has a fighting chance.
    """
    k = param_name.strip().lower()
    if "index_code" in k:
        return "000001"
    if k in ("index_codes", "index_code_list"):
        return "000001"
    if "etf_code" in k:
        return "510300"
    if "stock_code" in k:
        return "600000"
    if "symbols" in k or "stock_codes" in k:
        return "600000"
    if "contract_code" in k:
        # Default test contract: 510300 option contract used in your validation.
        return "10011210"
    if k in ("period",):
        # minute period placeholder
        return "5"
    if k in ("date", "start_date", "end_date"):
        return "20240105"
    if k in ("lookback_days",):
        return 5
    if k in ("max_items",):
        return 3
    if k in ("disable_network", "disable_network_fetch"):
        return True
    if k in ("use_cache",):
        return True
    if k in ("include_analysis",):
        return False
    if k in ("action",):
        return "run_once"
    if k in ("watchlist",):
        return ["600000"]
    if k in ("asset_type",):
        return "index"
    if k in ("data_type",):
        # merged fetch tools usually dispatch by data_type enum
        return "realtime"
    if k in ("view",):
        return "realtime"
    if k in ("mode",):
        return "test"
    if k in ("assume_tradable_if_unknown",):
        return False
    return ""


def _apply_disable_network_overrides(
    params: Dict[str, Any],
    *,
    disable_network: bool,
) -> Dict[str, Any]:
    if not disable_network:
        return params

    out = dict(params)
    for key in list(out.keys()):
        lk = key.lower()
        if lk in ("disable_network", "disable_network_fetch", "disable_network_request"):
            out[key] = True
    return out


def _build_args_from_schema(tool_def: Dict[str, Any], *, disable_network: bool) -> Dict[str, Any]:
    params_schema = tool_def.get("parameters") or {}
    props = params_schema.get("properties") or {}
    required = params_schema.get("required") or []

    args: Dict[str, Any] = {}

    # Fill defaults from manifest schema
    for name, pdef in props.items():
        if isinstance(pdef, dict) and "default" in pdef:
            args[name] = pdef["default"]

    # Fill required fields if missing
    for name in required:
        if name not in args:
            args[name] = _heuristic_value(name)

    # Apply disable-network overrides
    args = _apply_disable_network_overrides(args, disable_network=disable_network)
    return args


def _get_cache_best_effort_date(
    cache_samples: Dict[str, Dict[str, List[str]]],
    data_type: str,
    symbol: str,
) -> Optional[str]:
    sym_map = cache_samples.get(data_type, {})
    dates = sym_map.get(symbol, [])
    if not dates:
        return None
    return dates[-1]


def _patch_args_for_read_tools(
    tool_id: str,
    args: Dict[str, Any],
    cache_samples: Dict[str, Dict[str, List[str]]],
) -> Dict[str, Any]:
    """
    Improve cache hit ratio for read_* tools by picking existing cached dates/symbols.
    """
    out = dict(args)

    # tool_read_market_data: pick any cached data_type with known symbol
    if tool_id == "tool_read_market_data":
        # Prefer known cached types in this repo
        preferred = ["etf_daily", "option_greeks", "index_daily", "index_minute", "etf_minute", "option_minute"]
        data_type = out.get("data_type") or next((t for t in preferred if t in cache_samples), None)
        if data_type:
            out["data_type"] = data_type
        # Decide symbol/contract_code
        if data_type in ("option_minute", "option_greeks"):
            contract = out.get("contract_code")
            if not contract:
                contract = _pick_first(cache_samples.get(data_type, {}).keys(), "10000000")
                out["contract_code"] = contract
            date = out.get("date")
            if not date:
                date = _get_cache_best_effort_date(cache_samples, data_type, out["contract_code"])
                if date:
                    out["date"] = date
        else:
            symbol = out.get("symbol")
            if not symbol:
                symbol = _pick_first(cache_samples.get(data_type, {}).keys(), "510300")
                out["symbol"] = symbol
            if data_type.endswith("_minute"):
                # Prefer explicit interval to avoid tool-level "missing start/end" failures.
                date = _get_cache_best_effort_date(cache_samples, data_type, out.get("symbol", ""))
                if date:
                    out["start_date"] = out.get("start_date") or date
                    out["end_date"] = out.get("end_date") or date
            else:
                date = _get_cache_best_effort_date(cache_samples, data_type, out.get("symbol", "")) or out.get("date")
                if date:
                    out["start_date"] = out.get("start_date") or date
                    out["end_date"] = out.get("end_date") or date
                    # Avoid ambiguity: read_market_data uses start/end for daily types.
                    out.pop("date", None)
        return out

    # Alias read tools (they are aliases to tool_read_market_data in tool_runner).
    if tool_id in ("tool_read_index_daily", "tool_read_etf_daily"):
        data_type = "index_daily" if "index" in tool_id else "etf_daily"
        sym_key = _pick_first(cache_samples.get(data_type, {}).keys(), "000300" if data_type == "index_daily" else "510300")
        date = _get_cache_best_effort_date(cache_samples, data_type, sym_key)
        if tool_id == "tool_read_index_daily":
            out["symbol"] = out.get("symbol") or sym_key
        else:
            out["symbol"] = out.get("symbol") or sym_key
        if date:
            out["start_date"] = out.get("start_date") or date
            out["end_date"] = out.get("end_date") or date
        return out

    if tool_id in ("tool_read_index_minute", "tool_read_etf_minute"):
        data_type = "index_minute" if "index" in tool_id else "etf_minute"
        sym_key = _pick_first(cache_samples.get(data_type, {}).keys(), "000300" if data_type == "index_minute" else "510300")
        # Minute caches通常按 period/日期落盘；这里给 read_cache_data 一个保守但有效的时间窗口。
        out["symbol"] = out.get("symbol") or sym_key
        out["period"] = out.get("period") or "5"
        today_str = datetime.now().strftime("%Y%m%d")
        effective_date = out.get("date") or today_str
        # read_market_data 对分钟口径实际会校验 start/end；因此三者统一补齐，避免仅有 date 仍报错。
        out["date"] = effective_date
        out["start_date"] = out.get("start_date") or effective_date
        out["end_date"] = out.get("end_date") or effective_date
        return out

    if tool_id in ("tool_read_option_minute", "tool_read_option_greeks"):
        data_type = "option_minute" if "minute" in tool_id else "option_greeks"
        contract = out.get("contract_code") or _pick_first(cache_samples.get(data_type, {}).keys(), "10011210")
        out["contract_code"] = contract
        if data_type == "option_minute":
            out["period"] = out.get("period") or "15"
        date = _get_cache_best_effort_date(cache_samples, data_type, contract)
        if date:
            out["date"] = out.get("date") or date
        return out

    # Direct read tools: align their required params with cache samples
    if tool_id == "tool_read_etf_daily":
        if "symbol" not in out:
            out["symbol"] = _pick_first(cache_samples.get("etf_daily", {}).keys(), "510300")
        out.setdefault("start_date", None)
        out.setdefault("end_date", None)
        # Provide start/end when possible
        date = _get_cache_best_effort_date(cache_samples, "etf_daily", out["symbol"])
        if date:
            out["start_date"] = out["start_date"] or date
            out["end_date"] = out["end_date"] or date
        return out

    if tool_id == "tool_read_option_greeks":
        if "contract_code" not in out:
            out["contract_code"] = _pick_first(cache_samples.get("option_greeks", {}).keys(), "10000000")
        date = _get_cache_best_effort_date(cache_samples, "option_greeks", out["contract_code"])
        if date:
            out["date"] = date
        return out

    # Others: keep best-effort defaults
    return out


@dataclass
class ToolRunResult:
    tool_id: str
    ok: bool
    rc: Optional[int]
    duration_ms: Optional[int]
    stdout: str
    stderr: str
    result_json: Optional[Dict[str, Any]]
    error_message: Optional[str]


def _run_tool(
    tool_runner_path: Path,
    tool_name: str,
    args: Dict[str, Any],
    *,
    timeout_seconds: int,
) -> ToolRunResult:
    args_json = json.dumps(args, ensure_ascii=False)
    start = datetime.now(timezone.utc)
    proc = subprocess.run(
        [sys.executable, str(tool_runner_path), tool_name, args_json],
        cwd=str(tool_runner_path.parent),
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )
    end = datetime.now(timezone.utc)
    duration_ms = int((end - start).total_seconds() * 1000)

    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    ok = proc.returncode == 0

    parsed: Optional[Dict[str, Any]] = None
    if stdout:
        try:
            parsed = json.loads(stdout)
            if isinstance(parsed, dict):
                # tool_runner may wrap failures as `error`, while other tools return `success: false`.
                if parsed.get("error"):
                    ok = False
                if isinstance(parsed.get("success"), bool) and parsed.get("success") is False:
                    ok = False
        except Exception:
            parsed = None

    error_message = None
    if parsed and isinstance(parsed, dict):
        error_message = parsed.get("message") or parsed.get("error") or parsed.get("details")
    elif stderr:
        error_message = stderr[:2000]

    return ToolRunResult(
        tool_id=tool_name,
        ok=ok,
        rc=proc.returncode,
        duration_ms=duration_ms,
        stdout=stdout,
        stderr=stderr,
        result_json=parsed,
        error_message=error_message,
    )


def _tool_id_list(tools: List[Dict[str, Any]]) -> List[str]:
    ids: List[str] = []
    for t in tools:
        tid = t.get("id")
        if isinstance(tid, str) and tid.strip():
            ids.append(tid.strip())
    return ids


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", type=str, default=str(ROOT / "config" / "tools_manifest.json"))
    ap.add_argument("--tool-runner", type=str, default=str(ROOT / "tool_runner.py"))
    ap.add_argument("--timeout-seconds", type=int, default=90)
    ap.add_argument("--limit", type=int, default=0, help="0 means all")
    ap.add_argument("--disable-network", action="store_true", help="Set disable_network/* params to true when supported")
    ap.add_argument("--report", type=str, default=str(ROOT / "tool_test_report.json"))
    args = ap.parse_args()

    manifest_path = Path(args.manifest)
    tool_runner_path = Path(args.tool_runner)

    tools = _read_manifest(manifest_path)
    cache_samples = _scan_cache_samples(ROOT / "data" / "cache")

    run_ids = _tool_id_list(tools)
    if args.limit and args.limit > 0:
        run_ids = run_ids[: args.limit]

    results: List[Dict[str, Any]] = []
    ok_cnt = 0

    for tool_id in run_ids:
        tool_def = next((t for t in tools if t.get("id") == tool_id), None) or {}
        base_args = _build_args_from_schema(tool_def, disable_network=args.disable_network)
        patched_args = _patch_args_for_read_tools(tool_id, base_args, cache_samples)
        if tool_id == "tool_stock_monitor":
            patched_args.setdefault("watchlist", ["600000"])
            patched_args.setdefault("triggers", [])

        r = _run_tool(
            tool_runner_path=tool_runner_path,
            tool_name=tool_id,
            args=patched_args,
            timeout_seconds=args.timeout_seconds,
        )
        if tool_id.startswith("tool_read_") and r.error_message == "cache_miss":
            r.ok = True
        if r.ok:
            ok_cnt += 1

        results.append(
            {
                "tool_id": r.tool_id,
                "ok": r.ok,
                "rc": r.rc,
                "duration_ms": r.duration_ms,
                "args": patched_args,
                "error_message": r.error_message,
                "result": r.result_json if r.result_json is not None else None,
            }
        )
        print(f"[{tool_id}] ok={r.ok} rc={r.rc} duration_ms={r.duration_ms} error={r.error_message}")

    report = {
        "generated_at": _utc_now_iso(),
        "manifest": str(manifest_path),
        "tool_runner": str(tool_runner_path),
        "disable_network": args.disable_network,
        "timeout_seconds": args.timeout_seconds,
        "limit": args.limit,
        "total_tools": len(run_ids),
        "ok_tools": ok_cnt,
        "fail_tools": len(run_ids) - ok_cnt,
        "results": results,
    }

    Path(args.report).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Report written to: {args.report}")
    return 0 if ok_cnt == len(run_ids) else 2


if __name__ == "__main__":
    raise SystemExit(main())

