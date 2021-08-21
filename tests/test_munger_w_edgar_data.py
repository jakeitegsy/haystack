import unittest

from munger import Munger
from stock import Stock
from pandas.testing import assert_index_equal
from pandas import Index


class TestMungerWithEdgarData(unittest.TestCase):

    edgar = Stock(source='EDGAR', ticker='A').get_stock()
    
    def munger(self):
        return Munger(
            ticker='A',
            raw_data=self.edgar.raw_data,
            filename=self.edgar.filename,
            mappings=self.edgar.columns_mapping()
        )

    def test_convert_2000_new_year_to_1999_year_end(self):
        self.assertFalse(
            self.munger().convert_2000_new_year_to_1999_year_end(
                self.edgar.raw_data
            ).loc['2000-01-01'].values.any()
        )

    def test_set_uppercase_column_names(self):
        assert_index_equal(
            self.munger().set_uppercase_column_names(
                self.edgar.raw_data
            ).columns,
            self.edgar.raw_data.rename(
                str.upper, axis='columns'
            ).columns
        )

    def test_rename_columns(self):
        assert_index_equal(
            self.munger().rename_columns(
                dataframe=self.edgar.raw_data,
                mappings=self.edgar.columns_mapping(),
            ).columns,
            Index([
                'SYMBOL',
                'END_DATE',
                'AMEND',
                'PERIOD_FOCUS',
                'DOC_TYPE',
                'NET_REVENUE',
                'OP_INCOME',
                'NET_INCOME',
                'PER_SHARE_EARNINGS_BASIC',
                'PER_SHARE_EARNINGS_DILUTED',
                'PER_SHARE_DIVIDENDS',
                'NET_ASSETS',
                'CURRENT_ASSETS',
                'CURRENT_LIABILITIES',
                'NET_CASH',
                'NET_EQUITY',
                'NET_CASH_OP',
                'NET_CASH_INVESTED',
                'NET_CASH_FIN',
                'NET_DEBT',
                'NET_GOODWILL',
                'CAP_EX'
            ])
        )