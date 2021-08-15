import unittest
import re

from analyst import Analyst
from numpy import median
from pandas.testing import assert_index_equal
from pandas.api.types import is_numeric_dtype
from pandas import Index, MultiIndex, concat

analyst = Analyst(source='STOCKPUP', ticker='BAC').get_stock()
raw_data = analyst.get_raw_data()


class TestStockPup(unittest.TestCase):

	def test_source_folder_for_stockpup_data(self):
		self.assertEqual(
			Analyst(source='stockpup').get_stock().source_folder(), 
			'stockpup_data'
		)

	def test_stockpup_filename_when_ticker_provided(self):
		self.assertEqual(
			Analyst(ticker='BAC').get_stock().filename, 
			'BAC_quarterly_financial_data.csv'
		)

	def test_ticker_when_filename_is_given(self):
		self.assertEqual(
			Analyst(
				source='STOCKPUP', 
				filename='GOOG_quarterly_financial_data.csv'
			).get_stock().ticker, 
			re.search(r'(\w+)_quarterly', 'GOOG_quarterly_financial_data.csv')[1]
		)

	def test_raw_data_columns(self):
		assert_index_equal(
			raw_data.columns,
			Index([
				'Shares', 
				'Shares split adjusted', 
				'Split factor', 
				'Assets',
				'Current Assets', 
				'Liabilities', 
				'Current Liabilities',
				'Shareholders equity', 
				'Non-controlling interest', 
				'Preferred equity',
				'Goodwill & intangibles', 
				'Long-term debt', 
				'Revenue', 
				'Earnings',
				'Earnings available for common stockholders', 
				'EPS basic',
				'EPS diluted', 
				'Dividend per share', 
				'Cash from operating activities',
				'Cash from investing activities', 
				'Cash from financing activities',
				'Cash change during period', 
				'Cash at end of period',
				'Capital expenditures', 
				'Price', 
				'Price high', 
				'Price low', 
				'ROE',
				'ROA', 
				'Book value of equity per share', 
				'P/B ratio', 
				'P/E ratio',
				'Cumulative dividends per share', 
				'Dividend payout ratio',
				'Long-term debt to equity ratio', 
				'Equity to assets ratio',
				'Net margin', 
				'Asset turnover', 
				'Free cash flow per share',
				'Current ratio'
			])
		)

	def test_set_index_creates_multi_index_of_years_and_quarters(self):
		assert_index_equal(
			analyst.set_index(raw_data).index,
			MultiIndex.from_tuples([
				(2018, 1),
	            (2017, 4),
	            (2017, 3),
	            (2017, 2),
	            (2017, 1),
	            (2016, 4),
	            (2016, 3),
	            (2016, 2),
	            (2016, 1),
	            (2015, 4),
	            (2015, 3),
	            (2015, 2),
	            (2015, 1),
	            (2014, 4),
	            (2014, 3),
	            (2014, 2),
	            (2014, 1),
	            (2013, 4),
	            (2013, 3),
	            (2013, 2),
	            (2013, 1),
	            (2012, 4),
	            (2012, 3),
	            (2012, 2),
	            (2012, 1),
	            (2011, 4),
	            (2011, 3),
	            (2011, 2),
	            (2011, 1),
	            (2010, 4),
	            (2010, 3),
	            (2010, 2),
	            (2010, 1),
	            (2009, 4),
	            (2009, 3),
	            (2009, 2),
	            (2009, 1),
	            (2008, 4),
	            (2008, 3),
	            (2008, 2),
	            (2008, 1),
	            (2007, 4),
	            (2007, 3),
	            (2007, 2),
	            (2007, 1),
	            (2006, 4),
	            (2006, 3),
	            (2006, 2),
	            (2006, 1),
	            (2005, 4),
	            (2005, 3),
	            (2005, 2),
	            (2005, 1),
	            (2004, 4),
	            (2004, 3),
	            (2004, 2),
	            (2004, 1),
	            (2003, 4),
	            (2003, 3),
	            (2003, 2),
	            (2003, 1),
	            (2002, 4),
	            (2002, 3),
	            (2002, 2),
	            (2002, 1),
	            (2001, 4),
	            (2001, 3),
	            (2001, 2),
	            (2001, 1),
	            (2000, 4),
	            (2000, 3),
	            (2000, 2),
	            (2000, 1),
	            (1999, 4),
	            (1999, 3),
	            (1999, 2),
	            (1999, 1),
	            (1998, 4),
	            (1998, 3),
	            (1998, 2),
	            (1998, 1),
	            (1997, 4),
	            (1997, 3),
	            (1997, 2),
	            (1997, 1),
	            (1996, 4),
	            (1996, 3),
	            (1996, 2),
	            (1996, 1),
	            (1995, 4),
	            (1995, 3),
	            (1995, 2),
	            (1995, 1),
	            (1994, 4),
	            (1994, 3),
	            (1994, 2),
	            (1994, 1),
	            (1993, 4)
	        ], names=['YEAR', 'QUARTER'])
		)

	def test_get_annual_data_drops_years_without_4_quarters(self):
		annual_data = analyst.get_annual_data(raw_data)
		for year in annual_data.index.levels[0]:
			with self.subTest(i=year):
				try:
					self.assertEqual(len(annual_data[year], 4))
				except KeyError:
					continue

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

	def test_rename_columns_changes_column_names(self):
		assert_index_equal(
			analyst.rename_columns(raw_data).columns,
			Index([
				'NET_SHARES_NO_SPLIT',
				'NET_SHARES',
				'SPLIT FACTOR',
				'NET_ASSETS',
				'CURRENT_ASSETS',
				'NET_LIABILITIES',
				'CURRENT_LIABILITIES',
				'NET_EQUITY',
				'NET_NONCONTROLLING',
				'NET_PREFERRED',
				'NET_GOODWILL',
				'NET_DEBT',
				'NET_REVENUE',
				'EARNINGS',
				'NET_INCOME',
				'PER_SHARE_EARNINGS_BASIC',
				'PER_SHARE_EARNINGS_DILUTED',
				'PER_SHARE_DIVIDENDS',
				'NET_CASH_OP',
				'NET_CASH_INVESTED',
				'NET_CASH_FIN',
				'CASH CHANGE DURING PERIOD',
				'NET_CASH',
				'CAPITAL_EXPENDITURES',
				'PRICE',
				'PRICE HIGH',
				'PRICE LOW',
				'ROE',
				'ROA',
				'PER_SHARE_BOOK',
				'P/B RATIO',
				'P/E RATIO',
				'PER_SHARE_CUM_DIVIDENDS',
				'RATIO_DIVIDENDS',
				'LONG-TERM DEBT TO EQUITY RATIO',
				'EQUITY TO ASSETS RATIO',
				'NET MARGIN',
				'ASSET TURNOVER',
				'FREE CASH FLOW PER SHARE',
				'CURRENT RATIO'
			])
		)

	def test_set_numeric_datatypes(self):
		self.assertTrue(
			analyst.set_numeric_datatypes(raw_data).dtypes.apply(is_numeric_dtype).all(),
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

	def test_aggregate_keeps_same_columns_as_net_calculations(self):
		assert_index_equal(
			analyst.aggregate_quarters_to_years(raw_data).columns,
			analyst.calculate_net_values(raw_data).columns
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

	def test_calculate_moving_averages_per_share(self):
		assert_index_equal(
			analyst.calculate_moving_averages_per_share(raw_data).columns,
			Index([
				'PER_SHARE_CURRENT_ASSETS',
				'PER_SHARE_CURRENT_LIABILITIES',
				'PER_SHARE_NET_ASSETS',
				'PER_SHARE_NET_CASH',
				'PER_SHARE_NET_CASH_FIN',
				'PER_SHARE_NET_CASH_INVESTED',
				'PER_SHARE_NET_CASH_OP',
				'PER_SHARE_NET_DEBT',
				'PER_SHARE_NET_DIVIDENDS',
				'PER_SHARE_NET_EQUITY',
				'PER_SHARE_NET_EXPENSES',
				'PER_SHARE_NET_FCF',
				'PER_SHARE_NET_FCF_SHY',
				'PER_SHARE_NET_GOODWILL',
				'PER_SHARE_NET_INCOME',
				'PER_SHARE_NET_INVESTED_CAPITAL',
				'PER_SHARE_NET_LIABILITIES',
				'PER_SHARE_NET_NONCONTROLLING',
				'PER_SHARE_NET_RETAINED',
				'PER_SHARE_NET_REVENUE',
				'PER_SHARE_NET_SHARES',
				'PER_SHARE_NET_TANGIBLE',
				'PER_SHARE_NET_WORKING_CAPITAL',
				'PER_SHARE_DCF_HISTORIC',
				'PER_SHARE_DCF_SHY_HISTORIC'
			])
		)

	def test_calculate_average_per_share_averages(self):
		assert_index_equal(
			analyst.calculate_average_per_share_averages(raw_data).index,
			analyst.calculate_moving_averages_per_share(raw_data).columns
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