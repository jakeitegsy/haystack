import unittest

from tests.test_analyze_stock_data import TestData, pd

@unittest.skip
class TestDiffs(TestData):

    def setUp(self):
        super().setUp()

    def test_calc_diffs_retains_mavg_index(self):
        pd.testing.assert_index_equal(
            self.edgar.stock.diffs.index, self.edgar.stock.mavg.index
        )
        pd.testing.assert_index_equal(
            self.stockpup.stock.diffs.index, self.stockpup.stock.mavg.index
        )

    def test_calc_diffs_returns_dataframe_with_16_columns(self):
        self.assertEqual(len(self.edgar.stock.diffs.columns), 16)
        self.assertEqual(len(self.stockpup.stock.diffs.columns), 16)
        pd.testing.assert_index_equal(
            self.edgar.stock.diffs.columns,
            self.stockpup.stock.diffs.columns
        )
        self.assertEqual(
            sorted(self.edgar.stock.diffs.columns),
            ['DIFF_ASSETS_DEBT',
             'DIFF_ASSETS_LIABILITIES',
             'DIFF_CASH_DEBT',
             'DIFF_CASH_LIABILITIES',
             'DIFF_CURRENT_ASSETS_DEBT',
             'DIFF_CURRENT_ASSETS_LIABILITIES',
             'DIFF_EQUITY_DEBT',
             'DIFF_EQUITY_LIABILITIES',
             'DIFF_FCF_DEBT',
             'DIFF_FCF_LIABILITIES',
             'DIFF_FCF_SHY_DEBT',
             'DIFF_FCF_SHY_LIABILITIES',
             'DIFF_INCOME_DEBT',
             'DIFF_INCOME_LIABILITIES',
             'DIFF_TANGIBLE_DEBT',
             'DIFF_TANGIBLE_LIABILITIES']
        )