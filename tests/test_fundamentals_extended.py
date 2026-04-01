"""fundamentals_extended / unified_stock_views 的轻量单测（mock AkShare）。"""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd


class TestFundamentalsExtended(unittest.TestCase):
    def test_universe_uses_code_name_first(self):
        from plugins.data_collection.stock import fundamentals_extended as fe

        df = pd.DataFrame({"code": ["600000"], "name": ["浦发银行"]})
        fake_ak = MagicMock()
        fake_ak.stock_info_a_code_name.return_value = df

        with patch.object(fe, "AKSHARE_AVAILABLE", True), patch.object(fe, "ak", fake_ak):
            res = fe.tool_fetch_a_share_universe(max_rows=10)

        self.assertTrue(res.get("success"))
        self.assertEqual(res.get("provider"), "akshare")
        self.assertIn("stock_info_a_code_name", res.get("fallback_route", []))
        self.assertEqual(res.get("count"), 1)

    def test_financial_reports_bad_statement_type(self):
        from plugins.data_collection.stock import fundamentals_extended as fe

        with patch.object(fe, "AKSHARE_AVAILABLE", True):
            res = fe.tool_fetch_stock_financial_reports("600000", statement_type="bad")
        self.assertFalse(res.get("success"))

    def test_corporate_actions_requires_code_for_dividend(self):
        from plugins.data_collection.stock import fundamentals_extended as fe

        with patch.object(fe, "AKSHARE_AVAILABLE", True):
            res = fe.tool_fetch_stock_corporate_actions("dividend", stock_code="")
        self.assertFalse(res.get("success"))

    def test_universe_eastmoney_pref_tries_spot_first(self):
        from plugins.data_collection.stock import fundamentals_extended as fe

        df_spot = pd.DataFrame({"代码": ["600000"], "名称": ["浦发银行"]})
        fake_ak = MagicMock()
        fake_ak.stock_zh_a_spot_em.return_value = df_spot

        with patch.object(fe, "AKSHARE_AVAILABLE", True), patch.object(fe, "ak", fake_ak):
            res = fe.tool_fetch_a_share_universe(max_rows=5, provider_preference="eastmoney")

        self.assertTrue(res.get("success"))
        fake_ak.stock_zh_a_spot_em.assert_called()
        self.assertIn("stock_zh_a_spot_em", res.get("fallback_route", []))


class TestUnifiedStockViews(unittest.TestCase):
    def test_market_overview_partial_ok(self):
        from plugins.data_collection.stock import unified_stock_views as usv

        fake_ak = MagicMock()
        fake_ak.stock_sse_summary.return_value = pd.DataFrame({"a": [1]})
        fake_ak.stock_szse_summary.side_effect = RuntimeError("sz fail")

        with patch.object(usv, "AKSHARE_AVAILABLE", True), patch.object(usv, "ak", fake_ak):
            res = usv.fetch_stock_market_overview(trade_date="20240105")

        self.assertTrue(res.get("success"))
        self.assertIn("sse", res.get("data", {}))


if __name__ == "__main__":
    unittest.main()
