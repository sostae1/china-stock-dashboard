import unittest
from unittest.mock import patch
from datetime import datetime, timedelta

import pandas as pd
import pytz

import src.data_collector as dc


class TestFallbackLogic(unittest.TestCase):
    def setUp(self) -> None:
        # Reset circuit breaker state between tests
        dc._data_source_health.clear()

    def test_index_minute_fallback_order(self):
        """
        优先尝试 eastmoney；当其返回空时，降级到 sina（30m/15m 都应如此）。
        """
        config = {
            "data_sources": {
                "circuit_breaker": {"enabled": False},
                "index_minute": {
                    "priority": ["eastmoney", "sina"],
                    "eastmoney": {"enabled": True, "max_retries": 1, "retry_delay": 0.0},
                    "sina": {"enabled": True, "max_retries": 1, "retry_delay": 0.0},
                },
            }
        }

        market_status = {"is_trading_time": False}

        def fake_fetch_em(*, period: str, **kwargs):
            # eastmoney always returns empty
            return pd.DataFrame()

        def fake_fetch_sina(*, period: str, **kwargs):
            # sina returns a non-empty df
            return pd.DataFrame({"period": [period]})

        with (
            patch("src.config_loader.load_system_config", return_value=config),
            patch("src.system_status.get_current_market_status", return_value=market_status),
            patch.object(dc, "fetch_index_minute_em", side_effect=fake_fetch_em) as m_em,
            patch.object(dc, "fetch_index_minute_sina", side_effect=fake_fetch_sina) as m_sina,
        ):
            index_30m, index_15m = dc.fetch_index_minute_data_with_fallback(
                lookback_days=1, max_retries=1, retry_delay=0.0
            )

        self.assertIsNotNone(index_30m)
        self.assertFalse(index_30m.empty)
        self.assertEqual(index_30m["period"].iloc[0], "30")

        self.assertIsNotNone(index_15m)
        self.assertFalse(index_15m.empty)
        self.assertEqual(index_15m["period"].iloc[0], "15")

        # eastmoney and sina should be tried once for each period
        self.assertEqual(m_em.call_count, 2)
        self.assertEqual(m_sina.call_count, 2)

    def test_index_minute_circuit_breaker_skip_first_source(self):
        """
        当 circuit breaker 判定 eastmoney 处于熔断期时，直接跳过 eastmoney，使用 sina。
        """
        tz = pytz.timezone("Asia/Shanghai")
        now = datetime.now(tz)

        config = {
            "data_sources": {
                "circuit_breaker": {"enabled": True, "error_threshold": 1, "cooldown_seconds": 600},
                "index_minute": {
                    "priority": ["eastmoney", "sina"],
                    "eastmoney": {"enabled": True, "max_retries": 1, "retry_delay": 0.0},
                    "sina": {"enabled": True, "max_retries": 1, "retry_delay": 0.0},
                },
            }
        }

        market_status = {"is_trading_time": False}

        # Force open circuit for eastmoney for this test
        dc._data_source_health["index_minute_eastmoney"] = {
            "error_count": 1,
            "last_error_time": now,
            "circuit_open_until": now + timedelta(seconds=600),
        }

        def fake_fetch_em(*args, **kwargs):
            raise AssertionError("eastmoney should be skipped by circuit breaker")

        def fake_fetch_sina(*, period: str, **kwargs):
            return pd.DataFrame({"period": [period]})

        with (
            patch("src.config_loader.load_system_config", return_value=config),
            patch("src.system_status.get_current_market_status", return_value=market_status),
            patch.object(dc, "fetch_index_minute_em", side_effect=fake_fetch_em) as m_em,
            patch.object(dc, "fetch_index_minute_sina", side_effect=fake_fetch_sina) as m_sina,
        ):
            index_30m, index_15m = dc.fetch_index_minute_data_with_fallback(
                lookback_days=1, max_retries=1, retry_delay=0.0
            )

        self.assertIsNotNone(index_30m)
        self.assertFalse(index_30m.empty)
        self.assertEqual(index_30m["period"].iloc[0], "30")

        self.assertIsNotNone(index_15m)
        self.assertFalse(index_15m.empty)
        self.assertEqual(index_15m["period"].iloc[0], "15")

        # eastmoney should not be called at all
        self.assertEqual(m_em.call_count, 0)
        self.assertEqual(m_sina.call_count, 2)

    def test_index_minute_both_sources_fail_returns_empty(self):
        config = {
            "data_sources": {
                "circuit_breaker": {"enabled": False},
                "index_minute": {
                    "priority": ["eastmoney", "sina"],
                    "eastmoney": {"enabled": True, "max_retries": 1, "retry_delay": 0.0},
                    "sina": {"enabled": True, "max_retries": 1, "retry_delay": 0.0},
                },
            }
        }

        market_status = {"is_trading_time": False}

        def fake_fetch_fail(*args, **kwargs):
            return pd.DataFrame()

        with (
            patch("src.config_loader.load_system_config", return_value=config),
            patch("src.system_status.get_current_market_status", return_value=market_status),
            patch.object(dc, "fetch_index_minute_em", side_effect=fake_fetch_fail),
            patch.object(dc, "fetch_index_minute_sina", side_effect=fake_fetch_fail),
        ):
            index_30m, index_15m = dc.fetch_index_minute_data_with_fallback(
                lookback_days=1, max_retries=1, retry_delay=0.0
            )

        self.assertIsNotNone(index_30m)
        self.assertTrue(index_30m.empty)
        self.assertIsNotNone(index_15m)
        self.assertTrue(index_15m.empty)


if __name__ == "__main__":
    unittest.main()

