from tests.test_analyze_stock_data import TestData, pd


class TestRatios(TestData):

    def setUp(self):
        super().setUp()

    def test_calc_ratios_retains_mavg_index(self):
        self.assertEqual(
            sorted(self.stockpup.stock.ratios.index),
            sorted(self.stockpup.stock.mavg.index)
        )
        self.assertEqual(
            sorted(self.edgar.stock.ratios.index),
            sorted(self.edgar.stock.mavg.index)
        )

    def test_calc_ratios_returns_dataframe_with_37_columns(self):
        self.assertEqual(len(self.edgar.stock.ratios.columns), 36)
        self.assertEqual(len(self.stockpup.stock.ratios.columns), 36)
        pd.testing.assert_index_equal(
            self.edgar.stock.ratios.columns, 
            self.stockpup.stock.ratios.columns
        )
        self.assertEqual(
            sorted(self.edgar.stock.ratios.columns),
            ['RATIO_CASH_ASSETS',
             'RATIO_CASH_DEBT',
             'RATIO_CASH_LIABILITIES',
             'RATIO_CASH_TANGIBLE',
             'RATIO_CURRENT',
             'RATIO_CURRENT_ASSETS_LIABILITIES',
             'RATIO_CURRENT_DEBT',
             'RATIO_EQUITY_ASSETS',
             'RATIO_EQUITY_DEBT',
             'RATIO_EQUITY_LIABILITIES',
             'RATIO_EQUITY_TANGIBLE',
             'RATIO_EXPENSES_REVENUE',
             'RATIO_FCF_ASSETS',
             'RATIO_FCF_DEBT',
             'RATIO_FCF_EQUITY',
             'RATIO_FCF_EXPENSES',
             'RATIO_FCF_INVESTED_CAP',
             'RATIO_FCF_LIABILITIES',
             'RATIO_FCF_SHY_ASSETS',
             'RATIO_FCF_SHY_DEBT',
             'RATIO_FCF_SHY_EQUITY',
             'RATIO_FCF_SHY_EXPENSES',
             'RATIO_FCF_SHY_INVESTED_CAP',
             'RATIO_FCF_SHY_LIABILITIES',
             'RATIO_FCF_SHY_TANGIBLE',
             'RATIO_FCF_TANGIBLE',
             'RATIO_INCOME_ASSETS',
             'RATIO_INCOME_DEBT',
             'RATIO_INCOME_EQUITY',
             'RATIO_INCOME_EXPENSES',
             'RATIO_INCOME_INVESTED_CAP',
             'RATIO_INCOME_LIABILITIES',
             'RATIO_INCOME_TANGIBLE',
             'RATIO_TANGIBLE_ASSETS',
             'RATIO_TANGIBLE_DEBT',
             'RATIO_TANGIBLE_LIABILITIES',
            ]
        )