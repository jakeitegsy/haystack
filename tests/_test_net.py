import unittest

from tests.test_analyze_stock_data import TestData

@unittest.skip
class TestNetDf(TestData):

    def setUp(self):
        super().setUp()

    def test_data_has_unique_rows(self):
        self.assertFalse(self.edgar.stock.net.index.duplicated().all())
        self.assertFalse(self.stockpup.stock.net.index.duplicated().all())

    def test_calc_net_returns_dataframe_with_23_columns(self):
        self.assertEqual(len(self.edgar.stock.net.columns), 23)
        self.assertEqual(len(self.stockpup.stock.net.columns), 23)

    def test_column_names_of_net_dataframe_are_the_same(self):
        self.assertEqual(
            sorted(self.edgar.stock.net.columns),
            sorted(self.net_columns)
        )
        self.assertEqual(
            sorted(self.stockpup.stock.net.columns),
            sorted(self.edgar.stock.net.columns)
        )


