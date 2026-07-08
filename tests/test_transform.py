"""Unit tests for the transform layer.

These are deterministic and require no network or database access, so they run
in CI as a real correctness gate (not just an import smoke check).
"""
import unittest
from datetime import date, timedelta

import pandas as pd

from transform.indicators import (
    calculate_daily_return,
    calculate_moving_average,
    calculate_bullish_flag,
    calculate_price_change_pct,
    transform_market_data,
)


def _make_df(prices, symbol="bitcoin", start=date(2026, 1, 1)):
    """Build a minimal raw-shaped frame for a single coin."""
    rows = []
    for i, price in enumerate(prices):
        rows.append({
            "date": start + timedelta(days=i),
            "symbol": symbol,
            "price": float(price),
            "market_cap": float(price) * 1000,
            "volume": float(price) * 10,
        })
    return pd.DataFrame(rows)


class TransformTests(unittest.TestCase):
    def test_daily_return_first_row_is_nan_rest_are_pct(self):
        df = calculate_daily_return(_make_df([100, 110, 99]))
        returns = df.sort_values("date")["daily_return"].tolist()
        self.assertTrue(pd.isna(returns[0]))          # no prior day
        self.assertAlmostEqual(returns[1], 10.0)      # 100 -> 110 = +10%
        self.assertAlmostEqual(returns[2], -10.0)     # 110 -> 99  = -10%

    def test_moving_average_min_periods_one(self):
        df = calculate_moving_average(_make_df([10, 20, 30]), window=7)
        ma = df.sort_values("date")["ma_7"].tolist()
        self.assertAlmostEqual(ma[0], 10.0)           # avg of [10]
        self.assertAlmostEqual(ma[1], 15.0)           # avg of [10,20]
        self.assertAlmostEqual(ma[2], 20.0)           # avg of [10,20,30]

    def test_bullish_flag_matches_sign_of_return(self):
        df = calculate_bullish_flag(calculate_daily_return(_make_df([100, 110, 90])))
        flags = df.sort_values("date")["is_bullish"].tolist()
        self.assertFalse(bool(flags[1] is True and flags[0] is True))
        self.assertTrue(bool(flags[1]))               # +10% -> bullish
        self.assertFalse(bool(flags[2]))              # -18% -> not bullish

    def test_price_change_pct_matches_daily_return_math(self):
        df = calculate_price_change_pct(_make_df([50, 75]))
        pct = df.sort_values("date")["price_change_pct"].tolist()
        self.assertAlmostEqual(pct[1], 50.0)          # 50 -> 75 = +50%

    def test_per_symbol_isolation(self):
        # Returns must not leak across coins even when interleaved.
        btc = _make_df([100, 200], symbol="bitcoin")
        eth = _make_df([10, 5], symbol="ethereum")
        df = calculate_daily_return(pd.concat([btc, eth], ignore_index=True))
        btc_ret = df[(df.symbol == "bitcoin")].sort_values("date")["daily_return"].tolist()
        eth_ret = df[(df.symbol == "ethereum")].sort_values("date")["daily_return"].tolist()
        self.assertAlmostEqual(btc_ret[1], 100.0)     # 100 -> 200
        self.assertAlmostEqual(eth_ret[1], -50.0)     # 10 -> 5

    def test_full_pipeline_has_no_nulls_and_expected_columns(self):
        out = transform_market_data(_make_df([100, 105, 103, 108, 120]))
        expected = {
            "date", "symbol", "price", "market_cap", "volume",
            "daily_return", "ma_7", "volatility_7", "volatility",
            "price_change_pct", "is_bullish",
        }
        self.assertEqual(set(out.columns), expected)
        # clean_data fills NaNs; loader-critical columns must be null-free.
        self.assertFalse(out[list(expected)].isnull().any().any())

    def test_empty_input_returns_empty(self):
        out = transform_market_data(pd.DataFrame())
        self.assertTrue(out.empty)


if __name__ == "__main__":
    unittest.main()
