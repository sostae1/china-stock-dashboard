"""
L4：DTO 列名契约快照（mock 上游 DataFrame，断言 records 键集合稳定）。
上游增删列时须同步更新 tests/fixtures/l4/*.json 并在 PR 中说明。
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures" / "l4"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class TestDtoSnapshotsL4(unittest.TestCase):
    def test_financial_reports_record_keys(self):
        spec = _load("financial_reports_balance.json")
        cols = spec["mock_columns"]
        df = pd.DataFrame([dict(zip(cols, [1] * len(cols)))])
        fake_ak = MagicMock()
        fake_ak.stock_balance_sheet_by_report_em.return_value = df
        fake_ak.stock_financial_report_sina.side_effect = RuntimeError("skip")

        from plugins.data_collection.stock import fundamentals_extended as fe

        with patch.object(fe, "AKSHARE_AVAILABLE", True), patch.object(fe, "ak", fake_ak):
            res = fe.tool_fetch_stock_financial_reports("600000", statement_type="balance", provider_preference="eastmoney")

        self.assertTrue(res.get("success"), msg=res)
        row = res["data"][0]
        self.assertEqual(sorted(row.keys()), sorted(cols))

    def test_shareholders_top10_keys(self):
        spec = _load("stock_shareholders_top10.json")
        cols = spec["mock_columns"]
        df = pd.DataFrame([dict(zip(cols, [1, 2, 3]))])
        fake = MagicMock()
        fake.stock_main_stock_holder.return_value = df

        from plugins.data_collection.stock import reference_p1 as r1

        with patch.object(r1, "AKSHARE_AVAILABLE", True), patch.object(r1, "ak", fake):
            res = r1.tool_fetch_stock_shareholders("600000", holder_kind="top10")

        self.assertTrue(res.get("success"), msg=res)
        self.assertEqual(sorted(res["data"][0].keys()), sorted(cols))

    def test_index_constituents_keys(self):
        spec = _load("index_constituents.json")
        cols = spec["mock_columns"]
        df = pd.DataFrame([dict(zip(cols, ["c", "n", 0.01]))])
        fake = MagicMock()
        fake.index_stock_cons_weight_csindex.return_value = df

        from plugins.data_collection.stock import reference_p1 as r1

        with patch.object(r1, "AKSHARE_AVAILABLE", True), patch.object(r1, "ak", fake):
            res = r1.tool_fetch_index_constituents("000300", include_weight=True, provider_preference="csindex")

        self.assertTrue(res.get("success"), msg=res)
        self.assertEqual(sorted(res["data"][0].keys()), sorted(cols))


if __name__ == "__main__":
    unittest.main()
