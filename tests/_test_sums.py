import unittest

from tests.test_analyze_stock_data import TestData

@unittest.skip
class TestSums(TestData):

    def setUp(self):
        super().setUp()

    def test_sum_returns_series_with_mavg_columns_as_index(self):
        self.assertEqual(
            sorted(self.stockpup.stock.sums.index),
            sorted(self.stockpup.stock.mavg.columns)
        )
        self.assertEqual(
            sorted(self.edgar.stock.sums.index),
            sorted(self.stockpup.stock.sums.index)
        )