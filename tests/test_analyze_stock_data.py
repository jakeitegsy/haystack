import unittest

from haystack_stock import Edgar, Stockpup
from haystack_utilities import (
    ANALYSIS_FOLDER, EDGAR_FOLDER, STOCKPUP_FOLDER, PROCESSED_FOLDER, 
    TEST_FOLDER, pd, random_ticker, os, test_prep
)


class EdgarData:

    def __init__(self):
        self.to_folder = (f"{TEST_FOLDER}{PROCESSED_FOLDER}"
                          f"{ANALYSIS_FOLDER}{EDGAR_FOLDER}")
        
        self.stock = test_prep(
            from_folder=f"{PROCESSED_FOLDER}{EDGAR_FOLDER}",
            to_folder=self.to_folder,
            using=Edgar,
        )
        self.ticker = self.stock.ticker
        self.columns = [
            "amend",
            "assets",
            "cap_ex",
            "cash",
            "cash_flow_fin",
            "cash_flow_inv",
            "cash_flow_op",
            "cur_assets",
            "cur_liab",
            "debt",
            "dividend",
            "doc_type",
            "end_date",
            "eps_basic",
            "eps_diluted",
            "equity",
            "goodwill",
            "net_income",
            "op_income",
            "period_focus",
            "revenues",
            "symbol",
        ]


class StockpupData:

    def __init__(self):
        self.from_folder = f"{PROCESSED_FOLDER}{STOCKPUP_FOLDER}"
        self.to_folder = (f"{TEST_FOLDER}{PROCESSED_FOLDER}"
                          f"{ANALYSIS_FOLDER}{STOCKPUP_FOLDER}")

        self.stock = test_prep(
            from_folder = self.from_folder,
            to_folder=self.to_folder,
            using=Stockpup,
        )
        self.ticker = self.stock.ticker
        self.columns = [
            'Asset turnover',
            'Assets',
            'Book value of equity per share',
            'Capital expenditures',
            'Cash at end of period',
            'Cash change during period',
            'Cash from financing activities',
            'Cash from investing activities',
            'Cash from operating activities',
            'Cumulative dividends per share',
            'Current Assets',
            'Current Liabilities',
            'Current ratio',
            'Dividend payout ratio',
            'Dividend per share',
            'EPS basic',
            'EPS diluted',
            'Earnings',
            'Earnings available for common stockholders',
            'Equity to assets ratio',
            'Free cash flow per share',
            'Goodwill & intangibles',
            'Liabilities',
            'Long-term debt',
            'Long-term debt to equity ratio',
            'Net margin',
            'Non-controlling interest',
            'P/B ratio',
            'P/E ratio',
            'Preferred equity',
            'Price',
            'Price high',
            'Price low',
            'ROA',
            'ROE',
            'Revenue',
            'Shareholders equity',
            'Shares',
            'Shares split adjusted',
            'Split factor'
         ]


class TestData(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None
        self.edgar = EdgarData()
        self.stockpup = StockpupData()
        self.net_columns = [
            'CURRENT_ASSETS',
            'CURRENT_LIABILITIES',
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
        ]