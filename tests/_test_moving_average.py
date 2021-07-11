import unittest

from tests.test_analyze_stock_data import TestData

@unittest.skip
class TestMovingAVG(TestData):
    
    def setUp(self):
        super().setUp()

    def test_data_has_unique_rows(self):
        self.assertFalse(self.edgar.stock.mavg.index.duplicated().all())
        self.assertFalse(self.stockpup.stock.mavg.index.duplicated().all())

    def test_mavg_returns_dataframe_with_same_columns_same_as_net(self):
        self.assertEqual(
            len(self.edgar.stock.mavg.columns),
            len(self.stockpup.stock.net.columns)
        )
        self.assertEqual(
            len(self.edgar.stock.mavg.columns),
            len(self.stockpup.stock.mavg.columns)
        )
        self.assertEqual(
            sorted(self.edgar.stock.mavg.columns),
            sorted(self.stockpup.stock.mavg.columns)
        )
        self.assertEqual(
            sorted(self.edgar.stock.mavg.columns),
            sorted(self.net_columns)
        )