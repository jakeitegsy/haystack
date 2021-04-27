from tests.test_analyze_stock_data import TestData


class TestAverages(TestData):

    def setUp(self):
        super().setUp()

    def test_mavg_avg_returns_series_with_mavg_columns_as_index(self):
        self.assertEqual(
            sorted(self.stockpup.stock.mavg_avg.index), 
            sorted(self.stockpup.stock.mavg.columns)
        )
        self.assertEqual(
            sorted(self.edgar.stock.mavg_avg.index),
            sorted(self.stockpup.stock.mavg_avg.index)
        )

    def test_ratios_avg_returns_series_with_ratios_columns_as_index(self):
        self.assertEqual(
            sorted(self.stockpup.stock.ratios_avg.index),
            sorted(self.stockpup.stock.ratios.columns)
        )
        self.assertEqual(
            sorted(self.edgar.stock.ratios_avg.index),
            sorted(self.stockpup.stock.ratios_avg.index)
        )

    def test_diffs_avg_returns_series_with_diffs_columns_as_index(self):
        self.assertEqual(self.stockpup.stock.diffs_avg.shape, (16,))
        self.assertEqual(self.edgar.stock.diffs_avg.shape, (16,))
        self.assertEqual(
            sorted(self.stockpup.stock.diffs_avg.index),
            sorted(self.stockpup.stock.diffs.columns)
        )
        self.assertEqual(
            sorted(self.edgar.stock.diffs_avg.index),
            sorted(self.stockpup.stock.diffs_avg.index)
        )

    def test_per_share_diffs_avg_returns_series_with_prefixed_diffs_columns_as_index(self):
        self.assertEqual(
            sorted(self.edgar.stock.per_share_diffs_avg.index),
            sorted(self.stockpup.stock.per_share_diffs_avg.index)
        )
        self.assertEqual(
            sorted(self.stockpup.stock.per_share_diffs_avg.index),
            sorted(
                ["PER_SHARE_" + i for i in 
                    self.edgar.stock.diffs.columns]
            )
        )

    def test_growth_avg_returns_series_with_growth_columns_as_index(self):
        self.assertEqual(
            sorted(self.stockpup.stock.growth_avg.index),
            sorted(self.stockpup.stock.growth.columns)
        )
        self.assertEqual(
            sorted(self.edgar.stock.growth_avg.index),
            sorted(self.stockpup.stock.growth.columns)
        )

    def test_per_share_avg_returns_series_with_per_share_columns_as_index(self):
        self.assertEqual(
            sorted(self.stockpup.stock.per_share_avg.index),
            sorted(self.edgar.stock.per_share_avg.index)
        )
        self.assertEqual(
            sorted(self.edgar.stock.per_share_avg.index), 
            sorted(self.stockpup.stock.per_share.columns)
        )