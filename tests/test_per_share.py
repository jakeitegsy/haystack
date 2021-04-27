from tests.test_analyze_stock_data import TestData, pd


class TestPerShare(TestData):

    def setUp(self):
        super().setUp()

    def test_calc_per_share_returns_dataframe_with_25_columns(self):
        self.assertEqual(len(self.stockpup.stock.per_share.columns), 25)
        self.assertEqual(len(self.edgar.stock.per_share.columns), 25)
        self.assertEqual(
            sorted(self.edgar.stock.per_share.columns),
            sorted(self.stockpup.stock.per_share.columns)
        )
        self.assertEqual(
            sorted(self.stockpup.stock.per_share.columns),
            [
                'PER_SHARE_CURRENT_ASSETS',
                'PER_SHARE_CURRENT_LIABILITIES',
                'PER_SHARE_DCF_HISTORIC',
                'PER_SHARE_DCF_SHY_HISTORIC',
                'PER_SHARE_NET_ASSETS',
                'PER_SHARE_NET_CASH',
                'PER_SHARE_NET_CASH_FIN',
                'PER_SHARE_NET_CASH_INV',
                'PER_SHARE_NET_CASH_OP',
                'PER_SHARE_NET_DEBT',
                'PER_SHARE_NET_DIVIDENDS',
                'PER_SHARE_NET_EQUITY',
                'PER_SHARE_NET_EXPENSES',
                'PER_SHARE_NET_FCF',
                'PER_SHARE_NET_FCF_SHY',
                'PER_SHARE_NET_GOODWILL',
                'PER_SHARE_NET_INCOME',
                'PER_SHARE_NET_INVESTED_CAP',
                'PER_SHARE_NET_LIABILITIES',
                'PER_SHARE_NET_NONCONTROLLING',
                'PER_SHARE_NET_RETAINED',
                'PER_SHARE_NET_REVENUE',
                'PER_SHARE_NET_SHARES',
                'PER_SHARE_NET_TANGIBLE',
                'PER_SHARE_NET_WORKING_CAP'
            ]
        )

    def test_calc_per_share_retains_same_index_as_net(self):
        pd.testing.assert_index_equal(
            self.edgar.stock.per_share.index, self.edgar.stock.net.index
        )
        pd.testing.assert_index_equal(
            self.stockpup.stock.per_share.index,
            self.stockpup.stock.net.index
        )

    def test_per_share_diffs_retains_same_index_as_net(self):
        pd.testing.assert_index_equal(
            self.stockpup.stock.per_share_diffs.index, 
            self.stockpup.stock.net.index
        )
        pd.testing.assert_index_equal(
            self.edgar.stock.per_share_diffs.index,
            self.edgar.stock.net.index
        )

    def test_per_share_diffs_returns_diffs_columns_with_prefix(self):
        self.assertEqual(len(self.edgar.stock.per_share_diffs.columns), 16)
        self.assertEqual(len(self.stockpup.stock.per_share_diffs.columns), 16)
        self.assertEqual(
            sorted(self.edgar.stock.per_share_diffs.columns),
            sorted(
                ["PER_SHARE_" + i for i in self.edgar.stock.diffs.columns]
            )
        )
        self.assertEqual(
            sorted(self.stockpup.stock.per_share_diffs.columns),
            sorted(self.edgar.stock.per_share_diffs.columns)
        )      