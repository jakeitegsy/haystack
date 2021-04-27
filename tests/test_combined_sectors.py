import unittest

from haystack_sectors import Sectors
from haystack_utilities import (
    ANALYSIS_FOLDER, EDGAR_FOLDER, PROCESSED_FOLDER, SECTORS_FOLDER, 
    STOCKPUP_FOLDER, TEST_FOLDER, list_filetype, os, pd,
)

folder = f"{TEST_FOLDER}{PROCESSED_FOLDER}{ANALYSIS_FOLDER}"
sectors_folder = f"{folder}{SECTORS_FOLDER}"
edgar_folder = f"{folder}{EDGAR_FOLDER}"
stockpup_folder = f"{folder}{STOCKPUP_FOLDER}"
edgar_out = f"{sectors_folder}{EDGAR_FOLDER}"
stockpup_out = f"{sectors_folder}{STOCKPUP_FOLDER}"


def test_prep(sectors_folder=None, stocks_folder=None, to_folder=None):
    sectors = Sectors(
        sectors_folder=sectors_folder,
        stocks_folder=stocks_folder,
        to_folder=to_folder,
    )
    while sectors.stocks_df is None:
        test_prep(sectors_folder=sectors_folder, stocks_folder=stocks_folder,
                  to_folder=to_folder)

    return sectors


edgar_sectors = test_prep(sectors_folder=sectors_folder,
                          stocks_folder=edgar_folder,
                          to_folder=edgar_out)
stockpup_sectors = test_prep(sectors_folder=sectors_folder,
                             stocks_folder=stockpup_folder,
                             to_folder=stockpup_out)


class TestDataFrames(unittest.TestCase):

    def test_sectors_df_has_3_columns(self):
        self.assertEqual(len(edgar_sectors.sectors_df.columns), 3)
        self.assertEqual(len(stockpup_sectors.sectors_df.columns), 3)
        self.assertEqual(
            sorted(edgar_sectors.sectors_df.columns),
            sorted(stockpup_sectors.sectors_df.columns)
        )
        self.assertEqual(
            sorted(edgar_sectors.sectors_df.columns), 
            ["COMPANY", "SECTOR_NAME", "SECTOR_SYMBOL"]
        )

    def test_stocks_df_has_146_columns(self):
        self.assertEqual(len(edgar_sectors.stocks_df.columns), 146)
        self.assertEqual(len(stockpup_sectors.stocks_df.columns), 146)
        self.assertEqual(
            sorted(edgar_sectors.stocks_df.columns),
            sorted(stockpup_sectors.stocks_df.columns)
        )


class TestPrices(unittest.TestCase):

    def setUp(self):
        self.edgar_prices = edgar_sectors.symbols.PER_SHARE_MARKET
        self.stockpup_prices = stockpup_sectors.symbols.PER_SHARE_MARKET

    def test_get_current_price_returns_series(self):
        self.assertIs(type(self.edgar_prices), pd.Series)
        self.assertIs(type(self.stockpup_prices), pd.Series)

    def test_prices_is_a_series_with_the_same_index_as_combined_df(self):
        self.assertEqual(
            sorted(self.edgar_prices.index), 
            sorted(edgar_sectors.symbols.index)
        )
        self.assertEqual(
            sorted(self.stockpup_prices.index),
            sorted(stockpup_sectors.symbols.index)
        )


class TestPriceRatios(unittest.TestCase):

    def setUp(self):
        self.comparisons = [
            "RATIO_CASH_PRICE",
            "RATIO_DCF_HISTORIC_PRICE",
            "RATIO_DCF_SHY_HISTORIC_PRICE",
            "RATIO_DCF_FORWARD_PRICE",
            "RATIO_DCF_SHY_FORWARD_PRICE",
            "RATIO_EQUITY_PRICE",
            "RATIO_FCF_PRICE",
            "RATIO_FCF_SHY_PRICE",
            "RATIO_INCOME_PRICE",
            "RATIO_TANGIBLE_PRICE"
        ]

    def test_price_ratios_columns_are_added_to_combined_df(self):
        [self.assertIn(name, sorted(edgar_sectors.symbols.columns))
            for name in self.comparisons
        ]
        [self.assertIn(name, sorted(stockpup_sectors.symbols.columns))
            for name in self.comparisons
        ]

    def test_price_ratios_are_floats(self):
        [self.assertIs(
                type(edgar_sectors.symbols[f"{name}"]), pd.Series
            ) for name in self.comparisons
        ]


class TestAveragePriceScore(unittest.TestCase):

    def test_average_price_score_is_a_series(self):
        self.assertIs(
            type(edgar_sectors.symbols.AVERAGE_PRICE_RATIOS), pd.Series
        )
        self.assertIs(
            type(stockpup_sectors.symbols.AVERAGE_PRICE_RATIOS), pd.Series
        )
    

class TestReports(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None
        self.sectors = ["XL0", "XLB", "XLE", "XLF", "XLI", "XLK",
                               "XLP", "XLRE", "XLU", "XLV", "XLY"]

    def test_combined_report_has_161_columns(self):
        self.assertEqual(len(edgar_sectors.symbols.columns), 161)
        self.assertEqual(len(stockpup_sectors.symbols.columns), 161)
        self.assertEqual(
            sorted(edgar_sectors.symbols.columns),
            sorted(stockpup_sectors.symbols.columns)
        )
        self.assertEqual(
            sorted(edgar_sectors.symbols.columns),
            [
                'AVERAGE_GROWTH',
                'AVERAGE_PRICE_RATIOS',
                'AVERAGE_RETURNS',
                'AVERAGE_SAFETY',
                'COMPANY',
                'CURRENT_ASSETS',
                'CURRENT_LIABILITIES',
                'DATA_END',
                'DATA_START',
                'DIFF_ASSETS_DEBT',
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
                'DIFF_TANGIBLE_LIABILITIES',
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
                'NET_ASSETS',
                'NET_CASH',
                'NET_CASH_FIN',
                'NET_CASH_INV',
                'NET_CASH_OP',
                'NET_DEBT',
                'NET_DIVIDENDS',
                'NET_EQUITY',
                'NET_EXPENSES',
                'NET_FCF',
                'NET_FCF_SHY',
                'NET_GOODWILL',
                'NET_INCOME',
                'NET_INVESTED_CAP',
                'NET_LIABILITIES',
                'NET_NONCONTROLLING',
                'NET_RETAINED',
                'NET_REVENUE',
                'NET_SHARES',
                'NET_TANGIBLE',
                'NET_WORKING_CAP',
                'PER_SHARE_CURRENT_ASSETS',
                'PER_SHARE_CURRENT_LIABILITIES',
                'PER_SHARE_DCF_FORWARD',
                'PER_SHARE_DCF_HISTORIC',
                'PER_SHARE_DCF_SHY_FORWARD',
                'PER_SHARE_DCF_SHY_HISTORIC',
                'PER_SHARE_DIFF_ASSETS_DEBT',
                'PER_SHARE_DIFF_ASSETS_LIABILITIES',
                'PER_SHARE_DIFF_CASH_DEBT',
                'PER_SHARE_DIFF_CASH_LIABILITIES',
                'PER_SHARE_DIFF_CURRENT_ASSETS_DEBT',
                'PER_SHARE_DIFF_CURRENT_ASSETS_LIABILITIES',
                'PER_SHARE_DIFF_EQUITY_DEBT',
                'PER_SHARE_DIFF_EQUITY_LIABILITIES',
                'PER_SHARE_DIFF_FCF_DEBT',
                'PER_SHARE_DIFF_FCF_LIABILITIES',
                'PER_SHARE_DIFF_FCF_SHY_DEBT',
                'PER_SHARE_DIFF_FCF_SHY_LIABILITIES',
                'PER_SHARE_DIFF_INCOME_DEBT',
                'PER_SHARE_DIFF_INCOME_LIABILITIES',
                'PER_SHARE_DIFF_TANGIBLE_DEBT',
                'PER_SHARE_DIFF_TANGIBLE_LIABILITIES',
                'PER_SHARE_MARKET',
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
                'PER_SHARE_NET_WORKING_CAP',
                'RATIO_CASH_ASSETS',
                'RATIO_CASH_DEBT',
                'RATIO_CASH_LIABILITIES',
                'RATIO_CASH_PRICE',
                'RATIO_CASH_TANGIBLE',
                'RATIO_CURRENT',
                'RATIO_CURRENT_ASSETS_LIABILITIES',
                'RATIO_CURRENT_DEBT',
                'RATIO_DCF_FORWARD_PRICE',
                'RATIO_DCF_HISTORIC_PRICE',
                'RATIO_DCF_SHY_FORWARD_PRICE',
                'RATIO_DCF_SHY_HISTORIC_PRICE',
                'RATIO_EQUITY_ASSETS',
                'RATIO_EQUITY_DEBT',
                'RATIO_EQUITY_LIABILITIES',
                'RATIO_EQUITY_PRICE',
                'RATIO_EQUITY_TANGIBLE',
                'RATIO_EXPENSES_REVENUE',
                'RATIO_FCF_ASSETS',
                'RATIO_FCF_DEBT',
                'RATIO_FCF_EQUITY',
                'RATIO_FCF_EXPENSES',
                'RATIO_FCF_INVESTED_CAP',
                'RATIO_FCF_LIABILITIES',
                'RATIO_FCF_PRICE',
                'RATIO_FCF_SHY_ASSETS',
                'RATIO_FCF_SHY_DEBT',
                'RATIO_FCF_SHY_EQUITY',
                'RATIO_FCF_SHY_EXPENSES',
                'RATIO_FCF_SHY_INVESTED_CAP',
                'RATIO_FCF_SHY_LIABILITIES',
                'RATIO_FCF_SHY_PRICE',
                'RATIO_FCF_SHY_TANGIBLE',
                'RATIO_FCF_TANGIBLE',
                'RATIO_INCOME_ASSETS',
                'RATIO_INCOME_DEBT',
                'RATIO_INCOME_EQUITY',
                'RATIO_INCOME_EXPENSES',
                'RATIO_INCOME_INVESTED_CAP',
                'RATIO_INCOME_LIABILITIES',
                'RATIO_INCOME_PRICE',
                'RATIO_INCOME_TANGIBLE',
                'RATIO_TANGIBLE_ASSETS',
                'RATIO_TANGIBLE_DEBT',
                'RATIO_TANGIBLE_LIABILITIES',
                'RATIO_TANGIBLE_PRICE',
                'SECTOR_NAME',
                'SECTOR_SYMBOL'
            ]
        )

    def test_tickers_with_no_price_data_are_dropped(self):
        self.assertEqual(
            len(edgar_sectors.symbols[
                edgar_sectors.symbols.PER_SHARE_MARKET == 0
            ]), 0
        )
        self.assertEqual(
            len(stockpup_sectors.symbols[
                stockpup_sectors.symbols.PER_SHARE_MARKET == 0
            ]), 0
        )

    def test_reports_contains_11_groups_with_sector_symbol(self):
        self.assertEqual(
            sorted(list(edgar_sectors.reports.groups.keys())),
            sorted(list(stockpup_sectors.reports.groups.keys()))
        )
        self.assertEqual(
            sorted(list(edgar_sectors.reports.groups.keys())),
            sorted(self.sectors)
        )

    def test_write_reports_sector_files(self):
        self.assertTrue(os.path.exists(edgar_out))
        self.assertTrue(os.path.exists(stockpup_out))
        self.assertEqual(
            sorted(list_filetype(in_folder=edgar_out)),
            sorted(list_filetype(in_folder=stockpup_out))
        )
        self.assertEqual(
            sorted(list_filetype(in_folder=edgar_out)),
            sorted([sector + ".csv" for sector in self.sectors])
        )


if __name__ == "__main__":
    unittest.main()