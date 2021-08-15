import unittest

from analyst import Analyst
from pandas.testing import assert_index_equal
from pandas.api.types import is_numeric_dtype
from pandas import Index, concat
from numpy import median


analyst = Analyst(source='EDGAR', ticker='A').get_stock()
raw_data = analyst.get_raw_data()


class TestEdgar(unittest.TestCase):

    def test_source_folder_for_edgar_data(self):
        self.assertEqual(
            Analyst(
                source='edgar'
            ).get_stock().source_folder(), 
            'edgar_data'
        )

    def test_edgar_filename_when_ticker_provided(self):
        self.assertEqual(
            Analyst(
                ticker='GOOG', source='EDGAR'
            ).get_stock().filename, 
            'GOOG.csv'
        )

    def test_ticker_when_filename_is_given(self):
        self.assertEqual(
            Analyst(source='EDGAR', filename='GOOG.csv').get_stock().ticker, 
            'GOOG'
        )

    def test_set_index_creates_multi_index_of_years_and_quarters(self):
        assert_index_equal(
            analyst.set_index(raw_data).index,
            Index(
                [
                    2012, 2019, 2011, 2013, 2011, 2011, 2011, 2011, 
                    2012, 2013, 2013, 2014, 2014, 2012, 2013, 2009, 
                    2010, 2010, 2009, 2014, 2015, 2016, 2016, 2015, 
                    2014, 2010, 2015, 2015, 2016, 2016, 2017, 2017, 
                    2010, 2017, 2017, 2018, 2018, 2018, 2018
                ], 
                dtype='int64', name='YEAR'
            )
        )

    def test_replace_null_values_with_zero(self):
        self.assertFalse(
            analyst.replace_null_values_with_zero(raw_data).isna().values.any()
        )

    def test_convert_2000_new_year_to_1999_year_end(self):
        self.assertFalse(
            analyst.convert_2000_new_year_to_1999_year_end(raw_data).loc['2000-01-01'].values.any()
        )

    def test_set_uppercase_column_names(self):
        assert_index_equal(
            analyst.set_uppercase_column_names(raw_data).columns,
            raw_data.rename(str.upper, axis='columns').columns
        )

    def test_raw_data_columns(self):
        assert_index_equal(
            raw_data.columns,
            Index([
                'symbol', 
                'end_date', 
                'amend', 
                'period_focus', 
                'doc_type', 
                'revenues',
                'op_income', 
                'net_income', 
                'eps_basic', 
                'eps_diluted', 
                'dividend',
                'assets',
                'cur_assets', 
                'cur_liab', 
                'cash', 
                'equity', 
                'cash_flow_op',
                'cash_flow_inv', 
                'cash_flow_fin', 
                'debt', 
                'goodwill', 
                'cap_ex'
            ])
        )

    def test_renamed_columns(self):
        assert_index_equal(
            analyst.rename_columns(raw_data).columns,
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

    def test_set_numeric_datatypes(self):
        self.assertTrue(
            analyst.set_numeric_datatypes(raw_data)
                   .dtypes.apply(is_numeric_dtype).all()
        )

    def test_calculate_net_values(self):
        assert_index_equal(
            analyst.calculate_net_values(raw_data).columns,
            Index([
                'CURRENT_ASSETS',
                'CURRENT_LIABILITIES',
                'NET_ASSETS',
                'NET_CASH',
                'NET_CASH_FIN',
                'NET_CASH_INVESTED',
                'NET_CASH_OP',
                'NET_DEBT',
                'NET_DIVIDENDS',
                'NET_EQUITY',
                'NET_EXPENSES',
                'NET_FCF',
                'NET_FCF_SHY',
                'NET_GOODWILL',
                'NET_INCOME',
                'NET_INVESTED_CAPITAL',
                'NET_LIABILITIES',
                'NET_NONCONTROLLING',
                'NET_RETAINED',
                'NET_REVENUE',
                'NET_SHARES',
                'NET_TANGIBLE',
                'NET_WORKING_CAPITAL'
            ])
        )

    def test_moving_averages_keep_same_columns_as_net_calculations(self):
        assert_index_equal(
            analyst.calculate_moving_averages(raw_data).columns,
            analyst.calculate_net_values(raw_data).columns
        )

    def test_calculate_moving_average_differences(self):
        assert_index_equal(
            analyst.calculate_moving_average_differences(raw_data).columns,
            Index([
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
                'DIFF_TANGIBLE_LIABILITIES'
            ])
        )

    def test_calculate_growth_rates(self):
        assert_index_equal(
            analyst.calculate_moving_average_growth_rates(raw_data).index,
            Index([
                'GROWTH_CURRENT_ASSETS',
                'GROWTH_CURRENT_LIABILITIES',
                'GROWTH_NET_ASSETS',
                'GROWTH_NET_CASH',
                'GROWTH_NET_CASH_FIN',
                'GROWTH_NET_CASH_INVESTED',
                'GROWTH_NET_CASH_OP',
                'GROWTH_NET_DEBT',
                'GROWTH_NET_DIVIDENDS',
                'GROWTH_NET_EQUITY',
                'GROWTH_NET_EXPENSES',
                'GROWTH_NET_FCF',
                'GROWTH_NET_FCF_SHY',
                'GROWTH_NET_GOODWILL',
                'GROWTH_NET_INCOME',
                'GROWTH_NET_INVESTED_CAPITAL',
                'GROWTH_NET_LIABILITIES',
                'GROWTH_NET_NONCONTROLLING',
                'GROWTH_NET_RETAINED',
                'GROWTH_NET_REVENUE',
                'GROWTH_NET_SHARES',
                'GROWTH_NET_TANGIBLE',
                'GROWTH_NET_WORKING_CAPITAL',
            ])
        )

    def test_calculate_moving_average_ratios(self):
        assert_index_equal(
            analyst.calculate_moving_average_ratios(raw_data).columns,
            Index([
                'RATIO_CASH_ASSETS',
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
                'RATIO_FCF_INVESTED_CAPITAL',
                'RATIO_FCF_LIABILITIES',
                'RATIO_FCF_TANGIBLE',
                'RATIO_FCF_SHY_ASSETS',
                'RATIO_FCF_SHY_DEBT',
                'RATIO_FCF_SHY_EQUITY',
                'RATIO_FCF_SHY_EXPENSES',
                'RATIO_FCF_SHY_INVESTED_CAPITAL',
                'RATIO_FCF_SHY_LIABILITIES',
                'RATIO_FCF_SHY_TANGIBLE',
                'RATIO_INCOME_ASSETS',
                'RATIO_INCOME_DEBT',
                'RATIO_INCOME_EQUITY',
                'RATIO_INCOME_EXPENSES',
                'RATIO_INCOME_INVESTED_CAPITAL',
                'RATIO_INCOME_LIABILITIES',
                'RATIO_INCOME_TANGIBLE',
                'RATIO_TANGIBLE_ASSETS',
                'RATIO_TANGIBLE_LIABILITIES',
                'RATIO_TANGIBLE_DEBT'
            ])
        )

    def test_calculate_moving_average_sums(self):
        assert_index_equal(
            analyst.calculate_moving_average_sums(raw_data).index,
            analyst.calculate_moving_averages(raw_data).columns
        )

    def test_calculate_average_per_share_averages(self):
        assert_index_equal(
            analyst.calculate_average_per_share_averages(raw_data).index,
            analyst.calculate_moving_averages_per_share(raw_data).columns
        )

    def test_average_per_share_differences(self):
        assert_index_equal(
            analyst.calculate_average_per_share_differences(raw_data).index,
            Index([
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
                'PER_SHARE_DIFF_TANGIBLE_LIABILITIES'
            ])
        )

    def test_calculate_average_moving_averages(self):
        assert_index_equal(
            analyst.calculate_average_moving_averages(raw_data).index,
            analyst.calculate_moving_averages(raw_data).columns
        )

    def test_average_moving_average_differences(self):
        assert_index_equal(
            analyst.calculate_moving_average_differences(raw_data).columns,
            analyst.calculate_average_moving_average_differences(raw_data).index
        )

    def test_average_moving_average_ratios(self):
        assert_index_equal(
            analyst.calculate_average_moving_average_ratios(raw_data).index,
            analyst.calculate_moving_average_ratios(raw_data).columns
        )

    def test_average_per_share_differences(self):
        assert_index_equal(
            analyst.calculate_average_per_share_differences(raw_data).index,
            Index([
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
                'PER_SHARE_DIFF_TANGIBLE_LIABILITIES'
            ])
        )

    def test_calculate_median_growth_rate(self):
        growth_rates = analyst.calculate_moving_average_growth_rates(raw_data)
        self.assertEqual(
            analyst.calculate_median_growth_rate(raw_data),
            median([
                growth_rates['GROWTH_NET_ASSETS'],
                growth_rates['GROWTH_NET_CASH'],
                growth_rates['GROWTH_NET_CASH_INVESTED'],
                growth_rates['GROWTH_NET_EQUITY'],
                growth_rates['GROWTH_NET_FCF'],
                growth_rates['GROWTH_NET_FCF_SHY'],
                growth_rates['GROWTH_NET_INVESTED_CAPITAL'],
                growth_rates['GROWTH_NET_REVENUE'],
                growth_rates['GROWTH_NET_TANGIBLE'],
            ])
        )

    def test_calculate_median_returns(self):
        ratios = analyst.calculate_moving_average_ratios(raw_data)
        self.assertEqual(
            analyst.calculate_median_returns(raw_data),
            median([
                ratios['RATIO_INCOME_ASSETS'],
                ratios['RATIO_INCOME_EQUITY'],
                ratios['RATIO_INCOME_EXPENSES'],
                ratios['RATIO_INCOME_INVESTED_CAPITAL'],
                ratios['RATIO_INCOME_TANGIBLE'],
                ratios['RATIO_FCF_ASSETS'],
                ratios['RATIO_FCF_EQUITY'],
                ratios['RATIO_FCF_EXPENSES'],
                ratios['RATIO_FCF_INVESTED_CAPITAL'],
                ratios['RATIO_FCF_TANGIBLE'],
                ratios['RATIO_FCF_SHY_ASSETS'],
                ratios['RATIO_FCF_SHY_EQUITY'],
                ratios['RATIO_FCF_SHY_EXPENSES'],
                ratios['RATIO_FCF_SHY_INVESTED_CAPITAL'],
                ratios['RATIO_FCF_SHY_TANGIBLE'],
            ])
        )

    def test_calculate_median_safety(self):
        safety = analyst.calculate_average_moving_average_differences(raw_data)
        self.assertEqual(
            analyst.calculate_median_safety(raw_data),
            median([
                safety['DIFF_ASSETS_DEBT'],
                safety['DIFF_ASSETS_LIABILITIES'],
                safety['DIFF_CASH_DEBT'],
                safety['DIFF_CASH_LIABILITIES'],
                safety['DIFF_CURRENT_ASSETS_DEBT'],
                safety['DIFF_CURRENT_ASSETS_LIABILITIES'],
                safety['DIFF_EQUITY_DEBT'],
                safety['DIFF_EQUITY_LIABILITIES'],
                safety['DIFF_FCF_DEBT'],
                safety['DIFF_FCF_LIABILITIES'],
                safety['DIFF_FCF_SHY_DEBT'],
                safety['DIFF_FCF_SHY_LIABILITIES'],
                safety['DIFF_INCOME_DEBT'],
                safety['DIFF_INCOME_LIABILITIES'],
                safety['DIFF_TANGIBLE_DEBT'],
                safety['DIFF_TANGIBLE_LIABILITIES'],
                analyst.calculate_average_moving_averages(raw_data)['NET_WORKING_CAPITAL'],
            ])
        )

    def test_calculate_averages(self):
        assert_index_equal(
            analyst.calculate_averages(raw_data).index,
            Index([
                'AVERAGE_GROWTH',
                'AVERAGE_RETURNS',
                'AVERAGE_SAFETY',
                'DATA_END',
                'DATA_START',
                'PER_SHARE_DCF_FORWARD',
                'PER_SHARE_DCF_SHY_FORWARD'
            ])
        )

    def test_stock_summary(self):
        assert_index_equal(
            analyst.summarize(raw_data).index,
            concat([
                analyst.calculate_averages(raw_data),
                analyst.calculate_average_moving_average_differences(raw_data),
                analyst.calculate_moving_average_growth_rates(raw_data),
                analyst.calculate_average_moving_averages(raw_data),
                analyst.calculate_average_per_share_averages(raw_data),
                analyst.calculate_average_per_share_differences(raw_data),
                analyst.calculate_average_moving_average_ratios(raw_data),
            ], axis=0).index
        )