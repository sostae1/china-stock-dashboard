#!/usr/bin/env python3
"""
对比两次 scripts/test_all_tools.py 生成的报告，用于发版门禁：失败工具数增多则退出码 1。

用法:
  python scripts/compare_tool_reports.py tool_test_report_baseline.json tool_test_report.json

环境变量:
  COMPARE_STRICT=0  — 仅打印对比摘要，始终退出 0（便于 CI 过渡期）。
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Set


def _load(p: Path) -> Dict[str, Any]:
    return json.loads(p.read_text(encoding="utf-8"))


def _failed_ids(rep: Dict[str, Any]) -> Set[str]:
    out: Set[str] = set()
    for row in rep.get("results") or []:
        if not row.get("ok"):
            tid = row.get("tool_id")
            if isinstance(tid, str):
                out.add(tid)
    return out


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: compare_tool_reports.py <baseline.json> <current.json>", file=sys.stderr)
        return 2
    base_path = Path(sys.argv[1])
    cur_path = Path(sys.argv[2])
    if not base_path.is_file() or not cur_path.is_file():
        print("missing report file", file=sys.stderr)
        return 2

    base = _load(base_path)
    cur = _load(cur_path)
    bf = int(base.get("fail_tools") or 0)
    cf = int(cur.get("fail_tools") or 0)
    b_ids = _failed_ids(base)
    c_ids = _failed_ids(cur)
    new_fail = sorted(c_ids - b_ids)
    fixed = sorted(b_ids - c_ids)

    print(f"baseline fail_tools={bf} current fail_tools={cf}")
    print(f"newly_failing ({len(new_fail)}): {new_fail[:30]}{'...' if len(new_fail) > 30 else ''}")
    print(f"fixed ({len(fixed)}): {fixed[:30]}{'...' if len(fixed) > 30 else ''}")

    strict = (os.environ.get("COMPARE_STRICT") or "1").strip().lower() not in {"0", "false", "no"}
    if strict and cf > bf:
        print("Regression: more failing tools than baseline.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
