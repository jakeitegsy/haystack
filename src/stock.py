import datetime
import json
import yfinance as yahoo_finance

from datetime import datetime
from itertools import product
from re import search
from src.logger import Logger
from src.munger import Munger
from src.ratios import get_ratio
from pandas.errors import EmptyDataError
from pandas.api.types import is_object_dtype
from numpy import inf, nan, median
from scipy.stats import hmean
from pandas import (
    DataFrame, Series, MultiIndex, Index, read_csv, to_numeric,
    concat
)
from src.utilities import (
    analysis_folder, industry_folder, processed_folder,
    sectors_folder,  stockpup_folder,
    write_report,
)

def get_discount_cash_flow_value(values, discount_rate=0.0316):
    # how can I get the discount rate from the internet?
    return sum(
        [
            (values[i] / ((1 + discount_rate) ** i)) for i in range(len(values))
        ]
    )

class Stock:

    def __init__(self,
        ticker=None,
        filename=None,
        discount_rate=0.0316,
        source="STOCKPUP"
    ):
        self.discount_rate = discount_rate
        self.source = source.upper()
        self.ticker = ticker
        self.filename = filename
        self.logger = Logger(ticker)

    def stock_sources(self):
        return {
            'STOCKPUP': StockPup,
            'EDGAR': Edgar,
        }

    def get_stock(self):
        return self.stock_sources().get(self.source)(
            ticker=self.ticker, filename=self.filename
        )

    def get_net_equity(self, dataframe):
        return dataframe['NET_ASSETS'] - dataframe['NET_LIABILITIES']

    def get_net_dividends(self, dataframe):
        return dataframe['PER_SHARE_DIVIDENDS'] * self.get_net_shares(dataframe)

    def get_net_retained(self, dataframe):
        return dataframe['NET_INCOME'] * self.get_net_dividends(dataframe)

    def get_net_liabilities(self, dataframe):
        return dataframe["NET_ASSETS"] - dataframe["NET_EQUITY"]

    def get_net_tangible(self, dataframe):
        return (
            dataframe['NET_ASSETS']
          - self.get_net_liabilities(dataframe)
          + dataframe['NET_GOODWILL']
        )

    def get_munged_data(self):
        return self.set_numeric_datatypes(
            Munger(
                ticker=self.ticker,
                raw_data=self.get_raw_data(),
                mappings=self.columns_mapping(),
                filename=self.filename,
            ).munged_data
        )

    def get_net_values(self):
        self.logger.log('Calculating Net Values')
        munged_data = self.get_munged_data()
        return DataFrame({
            "CURRENT_ASSETS": munged_data["CURRENT_ASSETS"],
            "CURRENT_LIABILITIES": munged_data["CURRENT_LIABILITIES"],
            "NET_ASSETS": munged_data["NET_ASSETS"],
            "NET_CASH": munged_data["NET_CASH"],
            "NET_CASH_FIN": -munged_data["NET_CASH_FIN"],
            "NET_CASH_INVESTED": -munged_data["NET_CASH_INVESTED"],
            "NET_CASH_OP": munged_data["NET_CASH_OP"],
            "NET_DEBT": munged_data["NET_DEBT"],
            "NET_DIVIDENDS": self.get_net_dividends(munged_data),
            "NET_EQUITY": munged_data["NET_EQUITY"],
            "NET_EXPENSES": munged_data["NET_REVENUE"] - munged_data["NET_INCOME"],
            "NET_FCF": self.get_net_fcf(munged_data),
            "NET_FCF_SHY": munged_data["NET_CASH_OP"] + munged_data["NET_CASH_INVESTED"],
            "NET_GOODWILL": munged_data["NET_GOODWILL"],
            "NET_INCOME": munged_data["NET_INCOME"],
            "NET_INVESTED_CAPITAL": self.get_net_invested_capital(munged_data),
            "NET_LIABILITIES": self.get_net_liabilities(munged_data),
            "NET_NONCONTROLLING": self.get_net_noncontrolling(munged_data),
            "NET_RETAINED": self.get_net_retained(munged_data),
            "NET_REVENUE": munged_data["NET_REVENUE"],
            "NET_SHARES": self.get_net_shares(munged_data),
            "NET_TANGIBLE": self.get_net_tangible(munged_data),
            "NET_WORKING_CAPITAL": munged_data["CURRENT_ASSETS"] - munged_data["CURRENT_LIABILITIES"],
        })

    def raw_data_status(self):
        return f"Getting RAW data from {self.filename}::"

    def get_raw_data(self):
        try:
            self.logger.log(self.raw_data_status())
            return read_csv(
                f'{self.source_folder()}/{self.filename}',
                header=0,
                index_col=self.get_index_column(),
                parse_dates=True,
                infer_datetime_format=True,
                engine="c",
            )
        except (ValueError, EmptyDataError):
            self.logger.error(self.raw_data_status())

    def fourth_quarter(self):
        return lambda x: x[0]

    def get_differences(self, data):
        return {
            "DIFF_ASSETS_DEBT": data['NET_ASSETS'] - data['NET_DEBT'],
            "DIFF_ASSETS_LIABILITIES": data['NET_ASSETS'] - data['NET_LIABILITIES'],
            "DIFF_CASH_DEBT": data['NET_CASH'] - data['NET_DEBT'],
            "DIFF_CASH_LIABILITIES": data['NET_CASH'] - data['NET_LIABILITIES'],
            "DIFF_CURRENT_ASSETS_DEBT": data['CURRENT_ASSETS'] - data['NET_DEBT'],
            "DIFF_CURRENT_ASSETS_LIABILITIES": (
                data['CURRENT_ASSETS'] - data['NET_LIABILITIES']
            ),
            "DIFF_EQUITY_DEBT": data['NET_EQUITY'] - data['NET_DEBT'],
            "DIFF_EQUITY_LIABILITIES": data['NET_EQUITY'] - data['NET_LIABILITIES'],
            "DIFF_FCF_DEBT": data['NET_FCF'] - data['NET_DEBT'],
            "DIFF_FCF_LIABILITIES": data['NET_FCF'] - data['NET_LIABILITIES'],
            "DIFF_FCF_SHY_DEBT": data['NET_FCF_SHY']  - data['NET_DEBT'],
            "DIFF_FCF_SHY_LIABILITIES": data['NET_FCF_SHY']  - data['NET_LIABILITIES'],
            "DIFF_INCOME_DEBT": data['NET_INCOME']  - data['NET_DEBT'],
            "DIFF_INCOME_LIABILITIES": data['NET_INCOME']  - data['NET_LIABILITIES'],
            "DIFF_TANGIBLE_DEBT": data['NET_TANGIBLE']  - data['NET_DEBT'],
            "DIFF_TANGIBLE_LIABILITIES": data['NET_TANGIBLE']  - data['NET_LIABILITIES'],
        }

    def get_moving_average_differences(self):
        return DataFrame(
            self.get_differences(self.moving_averages)
        )

    def get_average_moving_average_differences(self):
        return Series(
            self.get_differences(
                self.get_average_moving_averages()
            )
        )

    def get_average_moving_average_ratios(self):
        return Series(
            self.get_ratios(
                self.get_moving_average_sums()
            )
        )

    def get_average_net_shares(self, dataframe):
        return self.get_average_moving_averages()['NET_SHARES']

    def get_average_per_share_differences(self):
        self.logger.log('Calculating Average Differences Per Share')
        return self.add_prefix(
            self.get_average_moving_average_differences()
                .div(
                    self.get_average_net_shares(
                        self.moving_averages
                    )
                )
        )

    def get_average_per_share_averages(self):
        self.logger.log('Calculating Moving Averages Per Share')
        return self.get_moving_averages_per_share().iloc[-1]

    def financial_ratios(self):
        return (
            'ASSETS', 'CASH', 'CASH_INVESTED', 'EQUITY', 'FCF', 'FCF_SHY',
            'INVESTED_CAPITAL', 'REVENUE', 'TANGIBLE'
        )

    def get_median_growth_rate(self):
        self.logger.log('Calculating Median Growth Rates')
        return median([
            self.get_moving_average_growth_rates()[f'GROWTH_NET_{value}']
            for value in self.financial_ratios()
        ])

    def get_median_returns(self):
        self.logger.log('Calculating Median Returns')
        moving_average_ratios = self.get_moving_average_ratios()
        return median([
            moving_average_ratios[f'RATIO_{value}']
            for value in (
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
        ])

    def obligations(self):
        return ('DEBT', 'LIABILITIES')

    def resources(self):
        return (
            'ASSETS',
            'CASH',
            'CURRENT_ASSETS',
            'EQUITY',
            'FCF',
            'FCF_SHY',
            'INCOME',
            'TANGIBLE'
        )

    def safety_pairs(self):
        return (
            '_'.join(column) for column in product(
                self.resources(),
                self.obligations()
            )
        )

    def get_median_safety(self):
        self.logger.log('Calculating Median Safety')
        safety = self.get_average_moving_average_differences()
        return median([
            *(safety[f'DIFF_{value}'] for value in self.safety_pairs()),
            self.get_average_moving_averages()['NET_WORKING_CAPITAL']
        ])

    def get_data_end(self):
        try:
            return self.moving_averages.index[0]
        except IndexError:
            return 0

    def get_data_start(self):
        try:
            return self.moving_averages.index[1]
        except IndexError:
            return 0

    def get_averages(self):
        self.logger.log('Calculating Average Scores')
        return Series({
            'AVERAGE_GROWTH': self.get_median_growth_rate(),
            'AVERAGE_RETURNS': self.get_median_returns(),
            'AVERAGE_SAFETY': self.get_median_safety(),
            'DATA_END': self.get_data_end(),
            'DATA_START': self.get_data_start(),
            'PER_SHARE_DCF_FORWARD': (
                self.get_average_per_share_averages()['PER_SHARE_NET_FCF']
              / self.discount_rate),
            'PER_SHARE_DCF_SHY_FORWARD': (
                self.get_average_per_share_averages()['PER_SHARE_NET_FCF_SHY']
              / self.discount_rate
            )
        })

    def get_compound_growth_rate(self, dataframe):
        try:
            return (
                (
                    (dataframe.iloc[-1] / dataframe.iloc[0])
                 ** (1.0 / (dataframe.shape[0] - 1))
                ) - 1
            )
        except (ZeroDivisionError, IndexError):
            return self.get_simple_growth_rate(dataframe)

    def get_simple_growth_rate(self, dataframe):
        self.logger.log('Calculating Compound Growth Rates FAILED::Using Simple Growth Rate Instead')
        return dataframe.pct_change(axis=0)

    def get_moving_average_growth_rates(self):
        return self.add_prefix(
            self.replace_null_values_with_zero(
                self.get_compound_growth_rate(
                    self.moving_averages
                )
            ),
            prefix='GROWTH_',
        )

    def get_dcf_valuation(self, dataframe=None, key=None):
        return (
            dataframe[key]
            .expanding(min_periods=1)
            .apply(get_discount_cash_flow_value, raw=True)
        )

    def add_prefix(self, dataframe, prefix='PER_SHARE_'):
        return dataframe.add_prefix(prefix)

    def get_moving_averages_per_share(self):
        result = self.add_prefix(
            self.moving_averages.div(
                self.moving_averages['NET_SHARES'], axis=0
            )
        )
        result['PER_SHARE_DCF_HISTORIC'] = self.get_dcf_valuation(
            dataframe=result,
            key='PER_SHARE_NET_FCF'
        )
        result['PER_SHARE_DCF_SHY_HISTORIC'] = self.get_dcf_valuation(
            dataframe=result,
            key='PER_SHARE_NET_FCF_SHY'
        )
        return result

    def get_ratios(self, data):
        return {
            "RATIO_CASH_ASSETS": data['NET_CASH'] / data['NET_ASSETS'],
            "RATIO_CASH_DEBT": get_ratio(data['NET_CASH'], data['NET_DEBT']),
            "RATIO_CASH_LIABILITIES": data['NET_CASH'] / data['NET_LIABILITIES'],
            "RATIO_CASH_TANGIBLE": get_ratio(data['NET_CASH'], data['NET_TANGIBLE']),
            "RATIO_CURRENT": get_ratio(
                data['CURRENT_ASSETS'], data["CURRENT_LIABILITIES"]
            ),
            "RATIO_CURRENT_ASSETS_LIABILITIES": get_ratio(
                data['CURRENT_ASSETS'], data['NET_LIABILITIES']
            ),
            "RATIO_CURRENT_DEBT": get_ratio(data['CURRENT_ASSETS'], data['NET_DEBT']),
            "RATIO_EQUITY_ASSETS": data['NET_EQUITY'] / data['NET_ASSETS'],
            "RATIO_EQUITY_DEBT": get_ratio(data['NET_EQUITY'], data['NET_DEBT']),
            "RATIO_EQUITY_LIABILITIES": data['NET_EQUITY'] / data['NET_LIABILITIES'],
            "RATIO_EQUITY_TANGIBLE": get_ratio(data['NET_EQUITY'], data['NET_TANGIBLE']),
            "RATIO_EXPENSES_REVENUE": get_ratio(data['NET_EXPENSES'], data['NET_REVENUE']),
            "RATIO_FCF_ASSETS": data['NET_FCF'] / data['NET_ASSETS'],
            "RATIO_FCF_CASH": data['NET_FCF'] / data['NET_CASH'],
            "RATIO_FCF_CASH_INVESTED": data['NET_FCF'] / data['NET_CASH_INVESTED'],
            "RATIO_FCF_DEBT": get_ratio(data['NET_FCF'], data['NET_DEBT']),
            "RATIO_FCF_EQUITY": get_ratio(data['NET_FCF'], data['NET_EQUITY']),
            "RATIO_FCF_EXPENSES": data['NET_FCF'] / data['NET_EXPENSES'],
            "RATIO_FCF_INVESTED_CAPITAL": data['NET_FCF'] / data['NET_INVESTED_CAPITAL'],
            "RATIO_FCF_LIABILITIES": data['NET_FCF'] / data['NET_LIABILITIES'],
            "RATIO_FCF_TANGIBLE": get_ratio(data['NET_FCF'], data['NET_TANGIBLE']),
            "RATIO_FCF_SHY_ASSETS": data['NET_FCF_SHY'] / data['NET_ASSETS'],
            "RATIO_FCF_SHY_CASH": data['NET_FCF_SHY'] / data['NET_CASH'],
            "RATIO_FCF_SHY_CASH_INVESTED": data['NET_FCF_SHY'] / data['NET_CASH_INVESTED'],
            "RATIO_FCF_SHY_DEBT": get_ratio(data['NET_FCF_SHY'], data['NET_DEBT']),
            "RATIO_FCF_SHY_EQUITY": get_ratio(data['NET_FCF_SHY'], data['NET_EQUITY']),
            "RATIO_FCF_SHY_EXPENSES": data['NET_FCF_SHY'] / data['NET_EXPENSES'],
            "RATIO_FCF_SHY_INVESTED_CAPITAL": data['NET_FCF_SHY'] / data['NET_INVESTED_CAPITAL'],
            "RATIO_FCF_SHY_LIABILITIES": data['NET_FCF_SHY'] / data['NET_LIABILITIES'],
            "RATIO_FCF_SHY_TANGIBLE": get_ratio(data['NET_FCF_SHY'], data['NET_TANGIBLE']),
            "RATIO_INCOME_ASSETS": data['NET_INCOME'] / data['NET_ASSETS'],
            "RATIO_INCOME_CASH": data['NET_INCOME'] / data['NET_CASH'],
            "RATIO_INCOME_CASH_INVESTED": data['NET_INCOME'] / data['NET_CASH_INVESTED'],
            "RATIO_INCOME_DEBT": get_ratio(data['NET_INCOME'], data['NET_DEBT']),
            "RATIO_INCOME_EQUITY": get_ratio(data['NET_INCOME'], data['NET_EQUITY']),
            "RATIO_INCOME_EXPENSES": data['NET_INCOME'] / data['NET_EXPENSES'],
            "RATIO_INCOME_INVESTED_CAPITAL": data['NET_INCOME'] / data['NET_INVESTED_CAPITAL'],
            "RATIO_INCOME_LIABILITIES": data['NET_INCOME'] / data['NET_LIABILITIES'],
            "RATIO_INCOME_TANGIBLE": get_ratio(data['NET_INCOME'], data['NET_TANGIBLE']),
            "RATIO_TANGIBLE_ASSETS": data['NET_TANGIBLE'] / data['NET_ASSETS'],
            "RATIO_TANGIBLE_LIABILITIES": data['NET_TANGIBLE'] / data['NET_LIABILITIES'],
            "RATIO_TANGIBLE_DEBT": get_ratio(data['NET_TANGIBLE'], data['NET_DEBT']),
        }

    def get_moving_average_ratios(self):
        return DataFrame(self.get_ratios(self.moving_averages))

    def get_moving_average_sums(self):
        return self.moving_averages.apply(sum)



    def get_average_moving_averages(self):
        try:
            result = self.moving_averages.iloc[-1]
        except IndexError:
            result = self.moving_averages
        result['NET_TANGIBLE'] = self.get_net_tangible(result)
        # result['NET_TANGIBLE'] = (
        #     result['NET_ASSETS'] - result['NET_LIABILITIES'] - result['NET_GOODWILL']
        # )
        return result

    def get_summary(self):
        return concat([
            self.get_averages(),
            self.get_average_moving_average_differences(),
            self.get_moving_average_growth_rates(),
            self.get_average_moving_averages(),
            self.get_average_per_share_averages(),
            self.get_average_per_share_differences(),
            self.get_average_moving_average_ratios(),
        ], axis=0)

    def replace_null_values_with_zero(self, dataframe):
        self.logger.log(f"Replaced Null Values with 0")
        return dataframe.replace([inf, -inf, nan, 'None', 'NaN'], 0)

    def to_csv(self, output_folder):
        self.logger.log(f'Writing {self.ticker} summary to csv')
        write_report(
            data_frame=self.get_summary(),
            report='summary',
            to_file=self.ticker,
            to_folder=output_folder,
        )


class StockPup(Stock):

    def __init__(self, ticker=None, filename=None):
        self.ticker = ticker if ticker else self.get_ticker(filename)
        self.filename = filename if filename else self.get_filename(ticker)
        super().__init__(ticker=self.ticker, filename=self.filename)
        self.get_moving_averages()

    def get_filename(self, ticker):
        return f'{ticker}_quarterly_financial_data.csv'

    def get_ticker(self, filename):
        try:
            return search(r'(\w+)_quarterly', filename)[1]
        except TypeError:
            return

    def get_index_column(self):
        return 'Quarter end'

    def set_index(self, dataframe):
        self.logger.log(f"Setting Index to Years")
        if not isinstance(dataframe.index, MultiIndex):
            return dataframe.set_index([
                Index(dataframe.index.year, name="YEAR"),
                Index(dataframe.index.quarter, name="QUARTER")
            ])
        else:
            return dataframe

    def source_folder(self):
        return 'stockpup_data'

    def columns_mapping(self):
        return {
            "ASSETS": "NET_ASSETS",
            "BOOK VALUE OF EQUITY PER SHARE": "PER_SHARE_BOOK",
            "CASH AT END OF PERIOD": "NET_CASH",
            "CASH FROM FINANCING ACTIVITIES": "NET_CASH_FIN",
            "CASH FROM INVESTING ACTIVITIES": "NET_CASH_INVESTED",
            "CASH FROM OPERATING ACTIVITIES": "NET_CASH_OP",
            "CAPITAL EXPENDITURES": "CAPITAL_EXPENDITURES",
            "CUMULATIVE DIVIDENDS PER SHARE": "PER_SHARE_CUM_DIVIDENDS",
            "CURRENT ASSETS": "CURRENT_ASSETS",
            "CURRENT LIABILITIES": "CURRENT_LIABILITIES",
            "DIVIDEND PAYOUT RATIO": "RATIO_DIVIDENDS",
            "DIVIDEND PER SHARE": "PER_SHARE_DIVIDENDS",
            "EARNINGS AVAILABLE FOR COMMON STOCKHOLDERS":"NET_INCOME",
            "EPS BASIC": "PER_SHARE_EARNINGS_BASIC",
            "EPS DILUTED": "PER_SHARE_EARNINGS_DILUTED",
            "GOODWILL & INTANGIBLES": "NET_GOODWILL",
            "LIABILITIES": "NET_LIABILITIES",
            "LONG-TERM DEBT": "NET_DEBT",
            "NON-CONTROLLING INTEREST": "NET_NONCONTROLLING",
            "PREFERRED EQUITY": "NET_PREFERRED",
            "REVENUE": "NET_REVENUE",
            "SHAREHOLDERS EQUITY": "NET_EQUITY",
            "SHARES": "NET_SHARES_NO_SPLIT",
            "SHARES SPLIT ADJUSTED": "NET_SHARES",
        }

    def set_numeric_datatypes(self, dataframe):
        self.logger.log('Converting to Numeric DataTypes')
        return self.replace_null_values_with_zero(
            self.get_annual_data(dataframe)
        ).apply(to_numeric)

    def get_incomplete_years(self, dataframe):
        return [
            year for year in dataframe.index.levels[0]
            if len(dataframe.loc[year]) != 4
        ]

    def get_annual_data(self, dataframe):
        result = self.set_index(dataframe)
        return result.drop(
            self.get_incomplete_years(result),
            axis=0, level=0
        )

    def get_net_fcf(self, dataframe):
        return dataframe['NET_CASH_OP'] - dataframe['CAPITAL_EXPENDITURES']

    def get_net_noncontrolling(self, dataframe):
        return dataframe["NET_NONCONTROLLING"] + dataframe["NET_PREFERRED"]

    def get_net_invested_capital(self, dataframe):
        return (
             dataframe["NET_DEBT"] + dataframe["NET_PREFERRED"] +
             self.get_net_noncontrolling(dataframe) + dataframe["NET_EQUITY"]
        )

    def fourth_quarter(self, x):
        return x.iat[0]

    def aggregate_mappings(self):
        return {
            "CURRENT_ASSETS": self.fourth_quarter,
            "CURRENT_LIABILITIES": self.fourth_quarter,
            "NET_ASSETS": self.fourth_quarter,
            "NET_CASH": self.fourth_quarter,
            "NET_CASH_FIN": sum,
            "NET_CASH_INVESTED": sum,
            "NET_CASH_OP": sum,
            "NET_DEBT": self.fourth_quarter,
            "NET_DIVIDENDS": sum,
            "NET_EQUITY": self.fourth_quarter,
            "NET_EXPENSES": sum,
            "NET_FCF": sum,
            "NET_FCF_SHY": sum,
            "NET_GOODWILL": self.fourth_quarter,
            "NET_INCOME": sum,
            "NET_INVESTED_CAPITAL": self.fourth_quarter,
            "NET_LIABILITIES": self.fourth_quarter,
            "NET_NONCONTROLLING": self.fourth_quarter,
            "NET_RETAINED": sum,
            "NET_REVENUE": sum,
            "NET_SHARES": self.fourth_quarter,
            "NET_TANGIBLE": self.fourth_quarter,
            "NET_WORKING_CAPITAL": self.fourth_quarter,
        }

    def get_aggregated_years(self):
        self.logger.log('Aggregating quarters to years')
        return self.get_net_values().groupby(
            level='YEAR'
        ).agg(self.aggregate_mappings())

    def get_moving_averages(self, window=5):
        self.logger.log(f'Calculating {window} year moving averages for Net Values')
        self.moving_averages = (
            self.get_aggregated_years()
                .rolling(window=window, min_periods=1)
                .median()
        )

    def get_net_shares(self, dataframe):
        return dataframe["NET_SHARES"]


class Edgar(Stock):

    def __init__(self, ticker=None, filename=None):
        self.ticker = ticker if ticker else self.get_ticker(filename)
        self.filename = filename if filename else self.get_filename(ticker)
        super().__init__(ticker=self.ticker, filename=self.filename)
        self.get_moving_averages()

    def get_filename(self, ticker):
        return f'{ticker}.csv'

    def get_ticker(self, filename):
        try:
            return search(r'([a-zA-Z]+)_?', filename)[1]
        except TypeError:
            return

    def get_index_column(self):
        return 'fiscal_year'

    def set_index(self, dataframe):
        try:
            dataframe.index = dataframe.index.year
            dataframe.index.name = 'YEAR'
        except AttributeError:
            self.logger.error("Setting Index to Years::")
        else:
            self.logger.success("Setting Index to Years::")
        finally:
            return dataframe

    def source_folder(self):
        return 'edgar_data'

    def columns_mapping(self):
        return {
            "ASSETS": "NET_ASSETS",
            "CASH": "NET_CASH",
            "CASH_FLOW_FIN": "NET_CASH_FIN",
            "CASH_FLOW_INV": "NET_CASH_INVESTED",
            "CASH_FLOW_OP": "NET_CASH_OP",
            "CUR_ASSETS": "CURRENT_ASSETS",
            "CUR_LIAB": "CURRENT_LIABILITIES",
            "DEBT": "NET_DEBT",
            "DIVIDEND": "PER_SHARE_DIVIDENDS",
            "EPS_BASIC": "PER_SHARE_EARNINGS_BASIC",
            "EPS_DILUTED": "PER_SHARE_EARNINGS_DILUTED",
            "GOODWILL": "NET_GOODWILL",
            "INTANGIBLE": "NET_INTANGIBLE",
            "REVENUES": "NET_REVENUE",
            "EQUITY": "NET_EQUITY",
        }

    def doc_type(self):
        return 'doc_type'

    def get_doc_type(self, dataframe, doc_type):
        result = self.set_index(dataframe)
        try:
            return result[result[self.doc_type()] == doc_type]
        except KeyError:
            return result[result[self.doc_type().upper()] == doc_type]
        except Exception:
            return result

    def get_quarterly_data(self, dataframe):
        return self.get_doc_type(dataframe, '10-Q')

    def get_annual_data(self, dataframe):
        return self.get_doc_type(dataframe, '10-K')

    def drop_non_numeric_data_types(self, dataframe):
        return self.get_annual_data(dataframe).drop(
            columns=dataframe.dtypes[dataframe.dtypes.apply(is_object_dtype)].index.to_list()
        )

    def set_numeric_datatypes(self, dataframe):
        self.logger.log('Converting to Numeric DataTypes')
        return self.replace_null_values_with_zero(
            self.drop_non_numeric_data_types(dataframe)
        ).apply(to_numeric)

    def get_net_fcf(self, dataframe):
        return dataframe["NET_CASH_OP"] + dataframe["NET_CASH_INVESTED"]

    def get_net_noncontrolling(self, dataframe):
        return 0.0

    def get_net_invested_capital(self, dataframe):
        return dataframe["NET_DEBT"] + dataframe["NET_EQUITY"]

    def get_net_shares(self, dataframe):
        return dataframe["NET_INCOME"] / dataframe["PER_SHARE_EARNINGS_DILUTED"]

    def get_moving_averages(self, window=5):
        self.logger.log(f'Calculating {window} year moving averages for Net Values')
        self.moving_averages = (
            self.get_net_values()
                .rolling(window=window, min_periods=1)
                .median()
        )



class AssignSector(Stock):

    def __init__(
        self, ticker=None, filename=None,
        to_folder=analysis_folder(sectors_folder())
    ):
        super().__init__(ticker=ticker, filename=filename,
                         to_folder=to_folder, from_folder=sectors_folder)
        self.folder = to_folder
        self.sectors = {
            "XLY": "CONSUMER_DISCRETIONARY",
            "XLP": "CONSUMER_STAPLES",
            "XLE": "ENERGY",
            "XLF": "FINANCIALS",
            "XLV": "HEALTHCARE",
            "XLI": "INDUSTRIALS",
            "XLB": "MATERIALS",
            "XLRE": "REAL_ESTATE",
            "XLK": "TECHNOLOGY",
            "XLU": "UTILITIES"
        }

        self.symbols = read_csv(
            self.filename, header=1,
            usecols=["Symbol", "Company Name"], index_col="Symbol"
        )
        self.symbols = self.symbols.rename(columns={
            "Symbol": "SYMBOL", "Company Name": "COMPANY"
        })
        self.symbols["SECTOR"] = self.sectors[self.ticker]

        write_report(
            data_frame=self.symbols, report="Sectors",
            to_file=self.ticker, to_folder=self.folder,
        )


class AssignIndustry(Stock):

    def __init__(self, ticker=None, filename=None,
                 to_folder=f"{analysis_folder}{industry_folder}"):
        super().__init__(ticker=ticker, filename=filename,
                         to_folder=to_folder,
                         from_folder=industry_folder)
        self.folder = to_folder
        self.symbols = pandas.read_csv(
            self.filename,
            usecols=["Symbol", "Name", "Sector", "Industry"],
            index_col="Symbol"
        )
        self.symbols = self.symbols.rename(columns={
            "Symbol": "SYMBOL",
            "Name": "COMPANY",
            "Sector": "SECTOR",
            "Industry": "INDUSTRY",
        })

        write_report(
            data_frame=self.symbols, report="Industry",
            to_file=self.ticker, to_folder=self.folder,
        )