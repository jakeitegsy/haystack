import unittest

from tests.test_analyze_stock_data import TestData, pd

@unittest.skip
class TestScores(TestData):

    def setUp(self):
        super().setUp()

    def test_safety_score_is_float(self):
        self.assertIs(
            type(self.stockpup.stock.averages.AVERAGE_SAFETY),
            pd.np.float64
        )
        self.assertIs(
            type(self.edgar.stock.averages.AVERAGE_SAFETY),
            type(self.stockpup.stock.averages.AVERAGE_SAFETY)
        )

    def test_returns_score_is_float(self):
        self.assertIs(
            type(self.edgar.stock.averages.AVERAGE_RETURNS),
            pd.np.float64
        )
        self.assertIs(
            type(self.stockpup.stock.averages.AVERAGE_RETURNS),
            type(self.edgar.stock.averages.AVERAGE_RETURNS)
        )

    def test_growth_score_is_float(self):
        self.assertIs(
            type(self.edgar.stock.averages.AVERAGE_GROWTH),
            pd.np.float64
        )
        self.assertIs(
            type(self.stockpup.stock.averages.AVERAGE_GROWTH),
            type(self.edgar.stock.averages.AVERAGE_GROWTH)
        )