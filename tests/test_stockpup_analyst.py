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
			Analyst(
				source='stockpup'
			).get_stock().source_folder(), 
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

	def annual_data(self):
		return analyst.get_annual_data(raw_data)

	def test_get_annual_data_drops_years_without_4_quarters(self):
		for year in self.annual_data().index.levels[0]:
			with self.subTest(i=year):
				try:
					self.assertEqual(len(self.annual_data()[year], 4))
				except KeyError:
					continue

	def test_replace_null_values_with_zero(self):
		self.assertFalse(
			(
				analyst
					.replace_null_values_with_zero(
						raw_data
					)
				.isna()
				.values.any()
			)
		)

	def test_convert_2000_new_year_to_1999_year_end(self):
 		self.assertFalse(
 			(
				analyst
			 		.convert_2000_new_year_to_1999_year_end(
						raw_data
					)
			)
			.loc['2000-01-01']
			.values.any()
		)
	
	def test_set_uppercase_column_names(self):
		assert_index_equal(
			(
				analyst
					.set_uppercase_column_names(raw_data)
					.columns
			),
			(
				raw_data
					.rename(str.upper, axis='columns')
					.columns
			)
		)

	def test_rename_columns_changes_column_names(self):
		assert_index_equal(
			(
				analyst
					.rename_columns(raw_data)
					.columns
			),
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
			(
				analyst
					.set_numeric_datatypes(raw_data)
					.dtypes.apply(is_numeric_dtype)
					.all()
			)
		)

	def net_columns(self):
		return (
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
		)

	def test_calculate_net_values(self):
		assert_index_equal(
			(
				analyst
					.calculate_net_values(
					raw_data
				)
				.columns
			),
			Index(self.net_columns())
		)

	def test_aggregate_keeps_same_columns_as_net_calculations(self):
		assert_index_equal(
			(
				analyst
					.aggregate_quarters_to_years(
						raw_data
					)
					.columns
			),
			(
				analyst
					.calculate_net_values(
						raw_data
						)
					.columns
			)
		)

	def test_moving_averages_keep_same_columns_as_net_calculations(self):
		assert_index_equal(
			(
				analyst
					.calculate_moving_averages(raw_data)
					.columns
			),
			(
				analyst
					.calculate_net_values(raw_data)
				.columns
			)
		)

	def test_calculate_moving_average_differences(self):
		assert_index_equal(
			(
				analyst
					.calculate_moving_average_differences(
						raw_data
					)
					.columns
			),
			Index([
				f'DIFF_{column}' for column in (
					'ASSETS_DEBT',
					'ASSETS_LIABILITIES',
					'CASH_DEBT',
					'CASH_LIABILITIES',
					'CURRENT_ASSETS_DEBT',
					'CURRENT_ASSETS_LIABILITIES',
					'EQUITY_DEBT',
					'EQUITY_LIABILITIES',
					'FCF_DEBT',
					'FCF_LIABILITIES',
					'FCF_SHY_DEBT',
					'FCF_SHY_LIABILITIES',
					'INCOME_DEBT',
					'INCOME_LIABILITIES',
					'TANGIBLE_DEBT',
					'TANGIBLE_LIABILITIES'
				)
			])			
		)

	def test_calculate_growth_rates(self):
		assert_index_equal(
			(
				analyst
					.calculate_moving_average_growth_rates(
						raw_data
					)
				.index
			),
			Index([
				f'GROWTH_{column}' 
				for column in self.net_columns()
			])
		)

	def test_calculate_moving_average_ratios(self):
		assert_index_equal(
			(
				analyst
					.calculate_moving_average_ratios(
						raw_data
					)
					.columns
			),
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
					'FCF_DEBT',
					'FCF_EQUITY',
					'FCF_EXPENSES',
					'FCF_INVESTED_CAPITAL',
					'FCF_LIABILITIES',
					'FCF_TANGIBLE',
					'FCF_SHY_ASSETS',
					'FCF_SHY_DEBT',
					'FCF_SHY_EQUITY',
					'FCF_SHY_EXPENSES',
					'FCF_SHY_INVESTED_CAPITAL',
					'FCF_SHY_LIABILITIES',
					'FCF_SHY_TANGIBLE',
					'INCOME_ASSETS',
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

	def test_calculate_moving_averages_per_share(self):
		assert_index_equal(
			(
				analyst
					.calculate_moving_averages_per_share(
						raw_data
					)
					.columns
			),
			Index([
				f'PER_SHARE_{column}' for column in (
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
					'NET_WORKING_CAPITAL',
					'DCF_HISTORIC',
					'DCF_SHY_HISTORIC'
				)
			])
		)

	def test_calculate_average_per_share_averages(self):
		assert_index_equal(
			(
				analyst
					.calculate_average_per_share_averages(
						raw_data
					)
					.index
			),
			(
				analyst
					.calculate_moving_averages_per_share(
						raw_data
					)
				.columns
			)
		)

	def test_calculate_average_moving_averages(self):
		assert_index_equal(
			(
				analyst
					.calculate_average_moving_averages(
						raw_data
					)
					.index
			),
			(
				analyst
					.calculate_moving_averages(
						raw_data
					)
					.columns
			)
		)

	def test_average_moving_average_differences(self):
		assert_index_equal(
			(
				analyst
					.calculate_moving_average_differences(
						raw_data
					)
					.columns
			),
			(
				analyst
					.calculate_average_moving_average_differences(raw_data)
					.index
			)
		)

	def test_average_moving_average_ratios(self):
		assert_index_equal(
			(
				analyst
					.calculate_average_moving_average_ratios(
						raw_data
					)
					.index
			),
			(
				analyst
					.calculate_moving_average_ratios(
						raw_data
					)
				.columns
			)
		)

	def test_average_per_share_differences(self):
		assert_index_equal(
			(
				analyst
					.calculate_average_per_share_differences(
						raw_data
					)
				.index
			),
			Index([
				f'PER_SHARE_DIFF_{column}' for column in (
					'ASSETS_DEBT',
					'ASSETS_LIABILITIES',
					'CASH_DEBT',
					'CASH_LIABILITIES',
					'CURRENT_ASSETS_DEBT',
					'CURRENT_ASSETS_LIABILITIES',
					'EQUITY_DEBT',
					'EQUITY_LIABILITIES',
					'FCF_DEBT',
					'FCF_LIABILITIES',
					'FCF_SHY_DEBT',
					'FCF_SHY_LIABILITIES',
					'INCOME_DEBT',
					'INCOME_LIABILITIES',
					'TANGIBLE_DEBT',
					'TANGIBLE_LIABILITIES'
				)
			])
		)

	def test_calculate_median_growth_rate(self):
		growth_rates = analyst.calculate_moving_average_growth_rates(raw_data)
		
		self.assertEqual(
			analyst.calculate_median_growth_rate(raw_data),
			median([
				growth_rates[f'GROWTH_NET_{column}'] for column in (
					'ASSETS',
					'CASH',
					'CASH_INVESTED',
					'EQUITY',
					'FCF',
					'FCF_SHY',
					'INVESTED_CAPITAL',
					'REVENUE',
					'TANGIBLE',
				)
			])
		)

	def test_calculate_median_returns(self):
		ratios = analyst.calculate_moving_average_ratios(raw_data)

		self.assertEqual(
			analyst.calculate_median_returns(raw_data),
			median([
				ratios[f'RATIO_{column}'] for column in (
					'INCOME_ASSETS',
					'INCOME_EQUITY',
					'INCOME_EXPENSES',
					'INCOME_INVESTED_CAPITAL',
					'INCOME_TANGIBLE',
					'FCF_ASSETS',
					'FCF_EQUITY',
					'FCF_EXPENSES',
					'FCF_INVESTED_CAPITAL',
					'FCF_TANGIBLE',
					'FCF_SHY_ASSETS',
					'FCF_SHY_EQUITY',
					'FCF_SHY_EXPENSES',
					'FCF_SHY_INVESTED_CAPITAL',
					'FCF_SHY_TANGIBLE'
				)
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
				analyst.calculate_average_moving_averages(
					raw_data
					)['NET_WORKING_CAPITAL'],
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
				(
					analyst
					.calculate_average_moving_average_differences(
							raw_data
						)
				),
				(
					analyst
						.calculate_moving_average_growth_rates(raw_data)
				),
				(analyst.calculate_average_moving_averages(
						raw_data
					)
				),
				(
					analyst
						.calculate_average_per_share_averages(
							raw_data
						)
				),
				(
					analyst
						.calculate_average_per_share_differences(
							raw_data
					)
				),
				(
					analyst
						.calculate_average_moving_average_ratios(
							raw_data
						)
				),
			], axis=0).index
		)