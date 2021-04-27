from tests.test_analyze_stock_data import TestData


class TestSummaryDf(TestData):

    def setUp(self):
        super().setUp()
        self.edgar.summary = self.edgar.stock.summary
        self.stockpup.summary = self.stockpup.stock.summary

    def test_summary_index_has_unique_rows(self):
        self.assertFalse(self.edgar.summary.index.duplicated().all())
        self.assertFalse(self.stockpup.summary.index.duplicated().all())

    def test_summary_index_is_a_series(self):
        self.assertEqual(self.stockpup.summary.shape, (146,))
        self.assertEqual(self.edgar.summary.shape, (146,))
        self.assertEqual(
            sorted(self.stockpup.summary.index),
            sorted(self.edgar.summary.index)
        )
        self.assertEqual(
            sorted(self.edgar.stock.summary.index),
            sorted(
                list(self.stockpup.stock.averages.index)
              + list(self.stockpup.stock.diffs.columns)
              + list(self.stockpup.stock.growth.columns)
              + list(self.stockpup.stock.mavg.columns)
              + list(self.stockpup.stock.per_share.columns)
              + list(self.stockpup.stock.per_share_diffs.columns)
              + list(self.stockpup.stock.ratios.columns)
            )
        )