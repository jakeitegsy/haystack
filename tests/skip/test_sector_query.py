import unittest

from eden_sector import Sector
from eden_utilities import (
    ANALYSIS_FOLDER, EDGAR_FOLDER, PROCESSED_FOLDER, SECTORS_FOLDER,     
    STOCKPUP_FOLDER, TEST_FOLDER,
    janitor, list_filetype, os, pd, random_file, random_ticker, test_prep
)


class TestSectorQuery(unittest.TestCase):

    def setUp(self):
        folder = (f"{TEST_FOLDER}{PROCESSED_FOLDER}"
                  f"{ANALYSIS_FOLDER}{SECTORS_FOLDER}")
        self.stockpup_folder = f"{folder}{STOCKPUP_FOLDER}"
        self.edgar_folder = f"{folder}{EDGAR_FOLDER}"
        self.stockpup_out = f"{self.stockpup_folder}reports/"
        self.edgar_out = f"{self.edgar_folder}reports/"
        self.maxDiff = None

        self.edgar_sector = test_prep(
            from_folder=self.edgar_folder, 
            to_folder=self.edgar_out,
            using=Sector
        )
        self.stockpup_sector = test_prep(
            from_folder=self.stockpup_folder,
            to_folder=self.stockpup_out,
            using=Sector
        )

        self.edgar = self.edgar_sector.symbol
        self.stockpup = self.stockpup_sector.symbol


class TestScores(TestSectorQuery):

    def series_test(self, df):
        self.assertIs(type(df), pd.Series)
        self.assertEqual(str(df.dtypes), 'float64')

    def setUp(self):
        super().setUp()

    def test_safety_score(self):
        self.series_test(self.edgar.SCORE_SAFETY)
        self.series_test(self.stockpup.SCORE_SAFETY)

    def test_returns_score(self):
        self.series_test(self.edgar.SCORE_RETURNS)
        self.series_test(self.stockpup.SCORE_RETURNS)

    def test_growth_score(self):
        self.series_test(self.edgar.SCORE_GROWTH)
        self.series_test(self.stockpup.SCORE_GROWTH)

    def test_price_score(self):
        self.series_test(self.edgar.SCORE_PRICE)
        self.series_test(self.stockpup.SCORE_PRICE)

    def test_total_score(self):
        self.series_test(self.edgar.SCORE_TOTAL)
        self.series_test(self.stockpup.SCORE_TOTAL)


class TestSymbol(TestSectorQuery):

    def setUp(self):
        super().setUp()

    def test_report_returns_data_frame_with_166_columns(self):
        self.assertEqual(len(self.edgar.columns), 166)
        self.assertEqual(len(self.stockpup.columns), 166)
        self.assertEqual(
            sorted(self.edgar.columns),
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
                'SCORE_GROWTH',
                'SCORE_PRICE',
                'SCORE_RETURNS',
                'SCORE_SAFETY',
                'SCORE_TOTAL',
                'SECTOR_NAME',
                'SECTOR_SYMBOL'
            ]
        )


class TestReports(TestSectorQuery):

    def setUp(self):
        super().setUp()

    def test_top_x_positions_writes_file_to_folder(self):
        self.assertTrue(
            os.path.exists((f"{self.edgar_folder}"
                            f"{self.edgar_sector.ticker}.csv"))
        )
        self.assertTrue(
            os.path.exists((f"{self.stockpup_folder}"
                            f"{self.stockpup_sector.ticker}.csv"))
        )

    def test_top_x_positions_writes_txt_files_to_reports_folder_by_default(self):
        self.assertEqual(
            len(list_filetype(in_folder=(f"{self.edgar_out}"
                                         f"{self.edgar_sector.ticker}/"), 
                              extension="txt")),
            self.edgar.shape[0]
        )
        self.assertEqual(
            len(list_filetype(in_folder=(f"{self.stockpup_out}"
                                         f"{self.stockpup_sector.ticker}/"),
                              extension="txt")),
            self.stockpup.shape[0]
        )


if __name__ == "__main__":
    unittest.main()