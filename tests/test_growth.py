from tests.test_analyze_stock_data import TestData


class TestGrowthRate(TestData):

    def setUp(self):
        super().setUp()

    def test_yearly_growth_returns_same_number_of_columns_as_mavg(self):
        self.assertEqual(
            len(self.stockpup.stock.growth.columns),
            len(self.net_columns)
        )
        self.assertEqual(
            sorted(self.stockpup.stock.growth.columns),
            sorted(self.edgar.stock.growth.columns)
        )
        self.assertEqual(
            sorted(self.stockpup.stock.growth.columns),
            [
                'GROWTH_CURRENT_ASSETS',
                'GROWTH_CURRENT_LIABILITIES',
                'GROWTH_NET_ASSETS',
                'GROWTH_NET_CASH',
                'GROWTH_NET_CASH_FIN',
                'GROWTH_NET_CASH_INV',
                'GROWTH_NET_CASH_OP',
                'GROWTH_NET_DEBT',
                'GROWTH_NET_DIVIDENDS',
                'GROWTH_NET_EQUITY',
                'GROWTH_NET_EXPENSES',
                'GROWTH_NET_FCF',
                'GROWTH_NET_FCF_SHY',
                'GROWTH_NET_GOODWILL',
                'GROWTH_NET_INCOME',
                'GROWTH_NET_INVESTED_CAP',
                'GROWTH_NET_LIABILITIES',
                'GROWTH_NET_NONCONTROLLING',
                'GROWTH_NET_RETAINED',
                'GROWTH_NET_REVENUE',
                'GROWTH_NET_SHARES',
                'GROWTH_NET_TANGIBLE',
                'GROWTH_NET_WORKING_CAP',
             ]
        )