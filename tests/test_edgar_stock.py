import unittest

from stock import Stock
from pandas.testing import assert_index_equal
from pandas.api.types import is_numeric_dtype
from pandas import Index, concat
from numpy import median

stock = Stock(source='EDGAR', ticker='A').get_stock()



class TestEdgar(unittest.TestCase):

    def test_source_folder_for_edgar_data(self):
        self.assertEqual(
            stock.source_folder(), 
            'edgar_data'
        )
    
    def test_edgar_filename_when_ticker_provided(self):
        self.assertEqual(
            Stock(
                ticker='GOOG', source='EDGAR'
            ).get_stock().filename, 
            'GOOG.csv'
        )

    def test_ticker_when_filename_is_given(self):
        self.assertEqual(
            Stock(source='EDGAR', filename='GOOG.csv').get_stock().ticker, 
            'GOOG'
        )

    def test_set_index_creates_multi_index_of_years_and_quarters(self):
        assert_index_equal(
            stock.set_index(stock.get_raw_data()).index,
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

    def test_raw_data_columns(self):
        assert_index_equal(
            stock.get_raw_data().columns,
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

    def test_set_numeric_datatypes(self):
        self.assertTrue(
            stock.set_numeric_datatypes(stock.get_raw_data())
                   .dtypes.apply(is_numeric_dtype).all()
        )

    def test_get_net_values(self):
        assert_index_equal(
            stock.get_net_values().columns,
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
            stock.moving_averages.columns,
            stock.get_net_values().columns
        )

    def test_get_moving_average_differences(self):
        assert_index_equal(
            stock.get_moving_average_differences().columns,
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

    def test_get_growth_rates(self):
        assert_index_equal(
            stock.get_moving_average_growth_rates().index,
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

    def test_get_moving_average_ratios(self):
        assert_index_equal(
            stock.get_moving_average_ratios().columns,
            Index([
                f'RATIO_{column}' for column in (
					'CASH_ASSETS',
					'CASH_DEBT',
					'CASH_LIABILITIES',
					'CASH_TANGIBLE',
					'CURRENT',
					'CURRENT_ASSETS_LIABILITIES',
					'CURRENT_DEBT',
					'EQUITY_ASSETS',
					'EQUITY_DEBT',
					'EQUITY_LIABILITIES',
					'EQUITY_TANGIBLE',
					'EXPENSES_REVENUE',
					'FCF_ASSETS',
					'FCF_CASH',
					'FCF_CASH_INVESTED',
					'FCF_DEBT',
					'FCF_EQUITY',
					'FCF_EXPENSES',
					'FCF_INVESTED_CAPITAL',
					'FCF_LIABILITIES',
					'FCF_TANGIBLE',
					'FCF_SHY_ASSETS',
					'FCF_SHY_CASH',
					'FCF_SHY_CASH_INVESTED',
					'FCF_SHY_DEBT',
					'FCF_SHY_EQUITY',
					'FCF_SHY_EXPENSES',
					'FCF_SHY_INVESTED_CAPITAL',
					'FCF_SHY_LIABILITIES',
					'FCF_SHY_TANGIBLE',
					'INCOME_ASSETS',
					'INCOME_CASH',
					'INCOME_CASH_INVESTED',
					'INCOME_DEBT',
					'INCOME_EQUITY',
					'INCOME_EXPENSES',
					'INCOME_INVESTED_CAPITAL',
					'INCOME_LIABILITIES',
					'INCOME_TANGIBLE',
					'TANGIBLE_ASSETS',
					'TANGIBLE_LIABILITIES',
					'TANGIBLE_DEBT'
                )
            ])
        )

    def test_get_moving_average_sums(self):
        assert_index_equal(
            stock.get_moving_average_sums().index,
            stock.moving_averages.columns
        )

    def test_get_average_per_share_averages(self):
        assert_index_equal(
            stock.get_average_per_share_averages().index,
            stock.get_moving_averages_per_share().columns
        )

    def test_average_per_share_differences(self):
        assert_index_equal(
            stock.get_average_per_share_differences().index,
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

    def test_get_average_moving_averages(self):
        assert_index_equal(
            stock.get_average_moving_averages().index,
            stock.moving_averages.columns
        )

    def test_average_moving_average_differences(self):
        assert_index_equal(
            stock.get_moving_average_differences().columns,
            stock.get_average_moving_average_differences().index
        )

    def test_average_moving_average_ratios(self):
        assert_index_equal(
            stock.get_average_moving_average_ratios().index,
            stock.get_moving_average_ratios().columns
        )

    def test_average_per_share_differences(self):
        assert_index_equal(
            stock.get_average_per_share_differences().index,
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

    def test_get_median_growth_rate(self):
        growth_rates = stock.get_moving_average_growth_rates()
        self.assertEqual(
            stock.get_median_growth_rate(),
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

    def test_get_median_returns(self):
        moving_average_ratios = stock.get_moving_average_ratios()
        self.assertEqual(
            stock.get_median_returns(),
            median([
				moving_average_ratios[
					f'RATIO_{column}'] for column in (
						'INCOME_ASSETS',
						'INCOME_CASH',
						'INCOME_CASH_INVESTED',
						'INCOME_EQUITY',
						'INCOME_EXPENSES',
						'INCOME_INVESTED_CAPITAL',
						'INCOME_TANGIBLE',
						'FCF_ASSETS',
						'FCF_CASH',
						'FCF_CASH_INVESTED',
						'FCF_EQUITY',
						'FCF_EXPENSES',
						'FCF_INVESTED_CAPITAL',
						'FCF_TANGIBLE',
						'FCF_SHY_ASSETS',
						'FCF_SHY_CASH',
						'FCF_SHY_CASH_INVESTED',
						'FCF_SHY_EQUITY',
						'FCF_SHY_EXPENSES',
						'FCF_SHY_INVESTED_CAPITAL',
						'FCF_SHY_TANGIBLE'
					)
				]
			)
        )

    def test_get_median_safety(self):
        safety = stock.get_average_moving_average_differences()
        self.assertEqual(
            stock.get_median_safety(),
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
                stock.get_average_moving_averages()['NET_WORKING_CAPITAL'],
            ])
        )

    def test_get_averages(self):
        assert_index_equal(
            stock.get_averages().index,
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
            stock.get_summary().index,
            concat([
                stock.get_averages(),
                stock.get_average_moving_average_differences(),
                stock.get_moving_average_growth_rates(),
                stock.get_average_moving_averages(),
                stock.get_average_per_share_averages(),
                stock.get_average_per_share_differences(),
                stock.get_average_moving_average_ratios(),
            ], axis=0).index
        )