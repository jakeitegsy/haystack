import datetime
import json
import yfinance as yahoo_finance

from datetime import datetime
from re import search
from pandas import (
    DataFrame, Series, MultiIndex, Index, read_csv, to_numeric,
    concat
)
from pandas.errors import EmptyDataError
from pandas.api.types import is_object_dtype
from numpy import inf, nan, median
from scipy.stats import hmean
from haystack_utilities import (
    ANALYSIS_FOLDER, INDUSTRY_FOLDER, PROCESSED_FOLDER, 
    SECTORS_FOLDER,  STOCKPUP_FOLDER,
    pd, list_filetype, os, test_folder, makedir
)

def get_ratio(numerator, denominator):
    """returns ratio for positive numbers
        returns diff for denominators less than or equal to zero
    """
    x = numerator
    y = denominator

    if type(x) == pd.Series and type(y) == pd.Series:
        # returns -abs(y) when x is 0
        ratio1 = -abs(y[x == 0])
        # returns -abs(x/y) when y is negative
        ratio2 = -abs(x[(x != 0) & (y < 0)]  / y[(x != 0) & (y < 0)])
        ratio3 = -abs(x[(x < 0) & (y > 0)] / y[(x < 0) & (y > 0)])
        # Defines Zero Division
        ratio4 = -abs(x[(x < 0) & (y == 0)])
        ratio5 = x[(x > 0) & (y == 0)]
        # regular ratio
        ratio6 = x[(x > 0) & (y > 0)] / y[(x > 0) & (y > 0)]
        return pd.concat([ratio1, ratio2, ratio3,
                            ratio4, ratio5, ratio6],
                            axis=0)
    else:
        # returns -abs(y) when x is 0
        if x == 0: return -abs(y)
        # returns -abs(x/y) when y is negative
        if x != 0 and y < 0: return -abs(x / y)
        if x < 0 and y > 0: return -abs(x / y)
        # Defines Zero Division
        if x < 0 and y == 0: return -abs(x)
        if x > 0 and y == 0: return x
        # regular ratio
        if x > 0 and y > 0: return x / y

class Analyst:

    def __init__(self, 
        ticker=None, 
        filename=None, 
        to_folder=None,
        from_folder=f"{PROCESSED_FOLDER}{STOCKPUP_FOLDER}", 
        discount_rate=0.0316, 
        source="STOCKPUP"
    ):
        self.discount_rate = discount_rate
        self.source = source.upper()
        self.ticker = ticker
        self.filename = filename

    def default_status(self):
        return f"{datetime.now()}::Ticker::{self.ticker}::"

    def log(self, message):
        print(f'{self.default_status()}{message}')

    def error(self, message):
        print(f'::[ERROR]{self.default_status()}{message}FAILED::')

    def success(self, message):
        print(f'{self.default_status()}{message}Succeeded::')

    def stock_sources(self):
        return {
            'STOCKPUP': StockPup,
            'EDGAR': Edgar,
        }

    def get_stock(self):
        try:
            return self.stock_sources()[self.source](
                ticker=self.ticker, filename=self.filename
            )
        except KeyError:
            return

    def munge_data(self, dataframe):
        status = f"Munging data from File::{self.filename}::"
        if len(dataframe) > 1:
            result = self.convert_2000_new_year_to_1999_year_end(dataframe)
            result = self.replace_null_values_with_zero(result)
            result = self.rename_columns(result)
            result = self.set_numeric_datatypes(result)
            self.success(status)
            return result
        else:
            self.error(status)

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

    def calculate_net_values(self, dataframe):
        self.log('Calculating Net Values')
        munged_data = self.munge_data(dataframe)
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

    def get_raw_data(self):
        status = f"Getting RAW data from {self.filename}::"
        try:
            self.log(status)
            return read_csv(
                f'{self.source_folder()}/{self.filename}',
                header=0,
                index_col=self.get_index_column(),
                parse_dates=True,
                infer_datetime_format=True,
                engine="c",
            )
        except (ValueError, EmptyDataError):
            self.error(status)

    def convert_2000_new_year_to_1999_year_end(self, dataframe):
        self.log('Converted 1999-12-31 to 2000-01-01')
        return dataframe.replace('2000-01-01', '1999-12-31')

    def set_uppercase_column_names(self, dataframe):
        self.log('Setting Column Names to UPPERCASE Labels')
        return dataframe.rename(str.upper, axis='columns')

    def rename_columns(self, dataframe):
        self.log('Converting Column Names')
        return self.set_uppercase_column_names(dataframe).rename(
            columns=self.columns_mapping()
        )

    def fourth_quarter(self):
        return lambda x: x[0]

    def aggregate(self):
        """returns dataframe with yearly summary for each column"""
        Q4 = lambda x: x[0]
        try: 
            return self.net_calculations.groupby(level=0).agg({
                "CURRENT_ASSETS": self.fourth_quarter(), # Q4,
                "CURRENT_LIABILITIES": Q4,
                "NET_ASSETS": Q4,
                "NET_CASH": Q4,
                "NET_CASH_FIN": sum,
                "NET_CASH_INVESTED": sum,
                "NET_CASH_OP": sum,
                "NET_DEBT": Q4,
                "NET_EXPENSES": sum,
                "NET_FCF": sum,
                "NET_FCF_SHY": sum,
                "NET_GOODWILL": Q4,
                "NET_INCOME": sum,
                "NET_INVESTED_CAPITAL": Q4,
                "NET_LIABILITIES": Q4,
                "NET_NONCONTROLLING": Q4,
                "NET_REVENUE": sum,
                "NET_RETAINED": sum,
                "NET_EQUITY": Q4,
                "NET_SHARES": Q4,
                "NET_WORKING_CAPITAL": Q4,
                "NET_TANGIBLE": Q4,
                "NET_DIVIDENDS": sum,
            })
        except TypeError as error:
            write_error(self.filename, error)
        except KeyError:
            pass

    def calculate_differences(self, data):
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

    def calculate_moving_average_differences(self, dataframe):
        return DataFrame(
            self.calculate_differences(
                self.calculate_moving_averages(dataframe)
            )
        )

    def calculate_average_moving_average_differences(self, dataframe):
        return Series(
            self.calculate_differences(
                self.calculate_average_moving_averages(dataframe)
            )
        )

    def calculate_average_moving_average_ratios(self, dataframe):
        return Series(
            self.calculate_ratios(
                self.calculate_moving_average_sums(dataframe)
            )
        )

    def calculate_average_net_shares(self, dataframe):
        return self.calculate_average_moving_averages(dataframe)['NET_SHARES']

    def calculate_average_per_share_differences(self, dataframe):
        self.log('Calculating Average Differences Per Share')
        return self.add_prefix(
            self.calculate_average_moving_average_differences(dataframe)
                .div(self.calculate_average_net_shares(dataframe))
        )

    def calculate_average_per_share_averages(self, dataframe):
        self.log('Calculating Moving Averages Per Share')
        return self.calculate_moving_averages_per_share(dataframe).iloc[-1]

    def calculate_median_growth_rate(self, dataframe):
        self.log('Calculating Median Growth Rates')
        growth_rates = self.calculate_moving_average_growth_rates(dataframe)
        return median([
            growth_rates[f'GROWTH_NET_{value}'] for value in (
                'ASSETS', 'CASH', 'CASH_INVESTED', 'EQUITY', 'FCF', 'FCF_SHY',
                'INVESTED_CAPITAL', 'REVENUE', 'TANGIBLE'
            )
        ])

    def calculate_median_returns(self, dataframe):
        self.log('Calculating Median Returns')
        ratios = self.calculate_moving_average_ratios(dataframe)
        return median([
            ratios[f'RATIO_{value}'] for value in (
                'INCOME_ASSETS', 'INCOME_EQUITY', 'INCOME_EXPENSES', 
                'INCOME_INVESTED_CAPITAL', 'INCOME_TANGIBLE',
                'FCF_ASSETS', 'FCF_EQUITY', 'FCF_EXPENSES', 
                'FCF_INVESTED_CAPITAL', 'FCF_TANGIBLE',
                'FCF_SHY_ASSETS', 'FCF_SHY_EQUITY', 'FCF_SHY_EXPENSES',
                'FCF_SHY_INVESTED_CAPITAL', 'FCF_SHY_TANGIBLE',
            )
        ])

    def calculate_median_safety(self, dataframe):
        self.log('Calculating Median Safety')
        safety = self.calculate_average_moving_average_differences(dataframe)
        safety_values = [
            safety[f'DIFF_{value}'] for value in (
                'ASSETS_DEBT', 'ASSETS_LIABILITIES', 
                'CASH_DEBT', 'CASH_LIABILITIES',
                'CURRENT_ASSETS_DEBT', 'CURRENT_ASSETS_LIABILITIES',
                'EQUITY_DEBT', 'EQUITY_LIABILITIES',
                'FCF_DEBT', 'FCF_LIABILITIES',
                'FCF_SHY_DEBT', 'FCF_SHY_LIABILITIES',
                'INCOME_DEBT', 'INCOME_LIABILITIES',
                'TANGIBLE_DEBT', 'TANGIBLE_LIABILITIES'
            )
        ]
        safety_values.append(
            self.calculate_average_moving_averages(dataframe)['NET_WORKING_CAPITAL']
        )
        return median(safety_values)

    def calculate_averages(self, dataframe):
        self.log('Calculating Average Scores')
        per_share_averages = self.calculate_average_per_share_averages(dataframe)
        return Series({
            'AVERAGE_GROWTH': self.calculate_median_growth_rate(dataframe),
            'AVERAGE_RETURNS': self.calculate_median_returns(dataframe),
            'AVERAGE_SAFETY': self.calculate_median_safety(dataframe),
            'DATA_END': self.calculate_moving_averages(dataframe).index[0],
            'DATA_START': self.calculate_moving_averages(dataframe).index[1],
            'PER_SHARE_DCF_FORWARD': per_share_averages['PER_SHARE_NET_FCF'] / self.discount_rate,
            'PER_SHARE_DCF_SHY_FORWARD': per_share_averages['PER_SHARE_NET_FCF_SHY'] / self.discount_rate
        })

    def calculate_compound_growth_rate(self, dataframe):
        try:
            return (
                (
                    (dataframe.iloc[-1] / dataframe.iloc[0]) 
                        ** (1.0 / (dataframe.shape[0] - 1))) - 1
            )
        except ZeroDivisionError:
            return self.calculate_simple_growth_rate(dataframe)

    def calculate_simple_growth_rate(self, dataframe):
        self.log('Calculating Compound Growth Rates FAILED::Using Simple Growth Rate Instead')
        return dataframe.pct_change(axis=0)

    def calculate_moving_average_growth_rates(self, dataframe):
        return self.add_prefix(
            self.replace_null_values_with_zero(
                self.calculate_compound_growth_rate(
                    self.calculate_moving_averages(dataframe)
                )
            ),
            prefix='GROWTH_',
        )

    def get_discount_cash_flow_value(self, values):
        return sum([(values[i] / ((1 + self.discount_rate) ** i)) 
                    for i in range(len(values))])

    def calc_growth(self, dataframe, using="simple"):
        "Return Simple or Compound Growth Rate"
        if using == "compound":            
            try:
                avg = (((dataframe.iloc[-1] / dataframe.iloc[0]) 
                        ** (1 / (dataframe.shape[0] - 1))) - 1)
            except ZeroDivisionError:
                avg = dataframe.pct_change(axis=0)
        else:
            avg = dataframe.pct_change(axis=0)

        avg = self.replace_null_values_with_zero(avg)
        avg = self.correct_columns(avg, prefix="GROWTH_")
        return avg

    def calculate_net(self, data):
        return self.transform_dict(net_dict)

    def calculate_dcf_valuation(self, dataframe=None, key=None):
        return (
            dataframe[key].expanding(min_periods=1)
                          .apply(
                            self.get_discount_cash_flow_value, raw=True
                           )
        )

    def add_prefix(self, dataframe, prefix='PER_SHARE_'):
        return dataframe.add_prefix(prefix)

    def calculate_moving_averages_per_share(self, dataframe):
        moving_averages = self.calculate_moving_averages(dataframe)
        result = self.add_prefix(
            moving_averages.div(moving_averages['NET_SHARES'], axis=0)
        )
        result['PER_SHARE_DCF_HISTORIC'] = self.calculate_dcf_valuation(dataframe=result, key='PER_SHARE_NET_FCF')
        result['PER_SHARE_DCF_SHY_HISTORIC'] = self.calculate_dcf_valuation(dataframe=result, key='PER_SHARE_NET_FCF_SHY')
        return result

    def calculate_ratios(self, data):
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
            "RATIO_FCF_DEBT": get_ratio(data['NET_FCF'], data['NET_DEBT']),
            "RATIO_FCF_EQUITY": get_ratio(data['NET_FCF'], data['NET_EQUITY']),
            "RATIO_FCF_EXPENSES": data['NET_FCF'] / data['NET_EXPENSES'],
            "RATIO_FCF_INVESTED_CAPITAL": data['NET_FCF'] / data['NET_INVESTED_CAPITAL'],
            "RATIO_FCF_LIABILITIES": data['NET_FCF'] / data['NET_LIABILITIES'],
            "RATIO_FCF_TANGIBLE": get_ratio(data['NET_FCF'], data['NET_TANGIBLE']),
            "RATIO_FCF_SHY_ASSETS": data['NET_FCF_SHY'] / data['NET_ASSETS'],
            "RATIO_FCF_SHY_DEBT": get_ratio(data['NET_FCF_SHY'], data['NET_DEBT']),
            "RATIO_FCF_SHY_EQUITY": get_ratio(data['NET_FCF_SHY'], data['NET_EQUITY']),
            "RATIO_FCF_SHY_EXPENSES": data['NET_FCF_SHY'] / data['NET_EXPENSES'],
            "RATIO_FCF_SHY_INVESTED_CAPITAL": data['NET_FCF_SHY'] / data['NET_INVESTED_CAPITAL'],
            "RATIO_FCF_SHY_LIABILITIES": data['NET_FCF_SHY'] / data['NET_LIABILITIES'],
            "RATIO_FCF_SHY_TANGIBLE": get_ratio(data['NET_FCF_SHY'], data['NET_TANGIBLE']),
            "RATIO_INCOME_ASSETS": data['NET_INCOME'] / data['NET_ASSETS'],
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

    def calculate_moving_average_ratios(self, dataframe):
        return DataFrame(
            self.calculate_ratios(self.calculate_moving_averages(dataframe))
        )

    def calculate_moving_average_sums(self, dataframe):
        return self.calculate_moving_averages(dataframe).apply(sum)

    def calculate_average_moving_averages(self, dataframe):
        result = self.calculate_moving_averages(dataframe).iloc[-1]
        result['NET_TANGIBLE'] = (
            result['NET_ASSETS'] - result['NET_LIABILITIES'] - result['NET_GOODWILL']
        )
        return result

    def summarize(self, dataframe):
        return concat([
            self.calculate_averages(dataframe),
            self.calculate_average_moving_average_differences(dataframe),
            self.calculate_moving_average_growth_rates(dataframe),
            self.calculate_average_moving_averages(dataframe),
            self.calculate_average_per_share_averages(dataframe),
            self.calculate_average_per_share_differences(dataframe),
            self.calculate_average_moving_average_ratios(dataframe),
        ], axis=0)
        
    def calc_price_ratios(self, dataframe):
        ratios = {
            "RATIO_CASH_PRICE" : "PER_SHARE_NET_CASH",
            "RATIO_DCF_HISTORIC_PRICE" : "PER_SHARE_DCF_HISTORIC",
            "RATIO_DCF_SHY_HISTORIC_PRICE" : "PER_SHARE_DCF_SHY_HISTORIC",
            "RATIO_DCF_FORWARD_PRICE" : "PER_SHARE_DCF_FORWARD",
            "RATIO_DCF_SHY_FORWARD_PRICE" : "PER_SHARE_DCF_SHY_FORWARD",
            "RATIO_EQUITY_PRICE" : "PER_SHARE_NET_EQUITY",
            "RATIO_FCF_PRICE" : "PER_SHARE_NET_FCF",
            "RATIO_FCF_SHY_PRICE" : "PER_SHARE_NET_FCF_SHY",
            "RATIO_INCOME_PRICE" : "PER_SHARE_NET_INCOME",
            "RATIO_TANGIBLE_PRICE" : "PER_SHARE_NET_TANGIBLE",
        }
        for (ratio, numerator) in ratios.items():
            try:
                dataframe[ratio] = dataframe[numerator] / dataframe["PER_SHARE_MARKET"]
            except ZeroDivisionError:
                dataframe[ratio] = 0.0


    def combine_files(self, folder, ext="csv", header=None, 
                      axis=0, file_col="Symbol", ignore_index=False):
        "Returns dataframe of csv files in folder"
        file_col = file_col.upper()
        file_list = list_filetype(in_folder=folder, extension=ext)

        dataframe_list = []
        try:
            for afile in file_list:
                try:
                    dataframe = pd.read_csv(
                        f"{folder}{afile}", header=header, index_col=0,
                        squeeze=True
                    )
                    dataframe[f"{file_col}"] = afile[:-4]
                    dataframe_list.append(dataframe)
                except FileNotFoundError:
                    pass
        except TypeError:
            pass

        combined = None
        try:
            combined = pd.concat(dataframe_list, axis=axis,
                                ignore_index=ignore_index, sort=False)
        except ValueError:
            pass

        try:
            return combined.T.set_index(file_col)
        except (KeyError, AttributeError):
            return combined

    def replace_null_values_with_zero(self, dataframe):
        self.log(f"Replaced Null Values with 0")
        return dataframe.replace([inf, -inf, nan, 'None', 'NaN'], 0)

    def yahoo_finance_download(self, tickers=None, start=None):
        if tickers is not None and start is not None:
            return yahoo_finance.download(
                list(tickers),
                start=start,
                end=datetime.date.today(),
                as_panel=False,
                threading=16
            )

    def get_current_prices(self, symbols="", dataframe=None):
        """Return """
        try:
            tickers = dataframe.index
        except AttributeError:
            tickers = symbols
        
        today = datetime.date.today()
        prices_today = f"prices/{str(today)}_current_prices.pkl"
        try:
            current_prices = pd.read_pickle(prices_today)
        except FileNotFoundError:            
            try:
                prices = self.yahoo_finance_download(
                    tickers=tickers, start=today,
                )
            except ValueError:
                prices = self.yahoo_finance_download(
                    tickers=tickers,
                    start=today - datetime.timedelta(days=1),
                )
            current_prices = prices["Adj Close"].T.squeeze()
            current_prices.to_pickle(prices_today)
        except Exception:
            self.error("I could not download current prices, "
                  "using last known prices instead")
            last_known_prices = sorted(
                list_filetype(in_folder="prices", extension="pkl")
            )[-1]
            return pd.read_pickle(last_known_prices)

        return current_prices

    def get_ticker_and_filename(self, filename=None, ticker=None, 
                                folder=None):
        if filename is not None:
            ticker = os.path.split(filename)[1].split(".")[0]
            filename = filename
            folder = os.path.dirname(filename)
        elif folder is not None:
            ticker = ticker
            filename = f"{folder}{ticker}.csv"
        return (ticker, filename)

    def judge_growth(self, rate):
        try:
            if rate > 0: return 'grew'
            if rate == 0: return 'idled'
            if rate < 0: return 'shrank'
        except TypeError:
            return 'ignore'    

    def relative_scaler(self, series):
        return (series / (series.max() - series.min()))

    def score(self, safety=4, growth=2, price=1, dataframe=None):
        safety_score = dataframe.SCORE_SAFETY.copy()
        returns_score = dataframe.SCORE_RETURNS.copy()
        growth_score = dataframe.SCORE_GROWTH.copy()
        price_score = dataframe.SCORE_PRICE.copy()

        # reward
        safety_score[safety_score > 0] = safety
        growth_score[growth_score > 0] = growth
        price_score[price_score > 0] = price
        #penalty
        safety_score[safety_score <= 0] = 0
        growth_score[growth_score <= 0] = 0
        price_score[price_score <= 0] = 0

        score = sum(
            [safety_score, returns_score, growth_score, price_score]
        ) 
        return score / max(score)

    def score_price_ratios(self, dataframe):
        price_ratios = pd.np.mean([
            dataframe.RATIO_CASH_PRICE,
            dataframe.RATIO_DCF_SHY_FORWARD_PRICE,
            dataframe.RATIO_DCF_SHY_HISTORIC_PRICE,
            dataframe.RATIO_EQUITY_PRICE,
            dataframe.RATIO_INCOME_PRICE,
            dataframe.RATIO_TANGIBLE_PRICE,
        ], axis=0)

        return self.transform(price_ratios)

    def sort_index(self, dataframe):
        try:
            return dataframe.sort_index()
        except TypeError:
            return dataframe

    def transform_dict(self, dictionary):
        try:
            result = pd.DataFrame(dictionary)
        except ValueError:
            result = pd.Series(dictionary)
        except pd.core.indexes.base.InvalidIndexError:
            result = pd.DataFrame.from_dict(
                dictionary, orient='index', dtype=pd.np.float64
            ).T.drop_duplicates(keep='last')
        return sort_index(result)

    def set_uppercase_labels(self, dataframe):
        try:
            dataframe.rename(str.upper)
        except TypeError:
            dataframe.rename(str.upper, axis="columns")
        finally:
            return dataframe

    def write_report(self, dataframe=None, report='', to_file=None, 
                     to_folder=None):
        makedir(to_folder)
        filename = f'{to_folder}{to_file}.csv'

        try:
            dataframe.sort_index(axis=1).to_csv(filename, header=True)
        except (TypeError, ValueError):
            dataframe.sort_index().to_csv(filename, header=True)
        except AttributeError:
            return "Invalid operation for NoneType"
        except FileNotFoundError:
            self.write_report(dataframe=dataframe, report=report,
                              to_file=to_file, to_folder=to_folder)
        
        self.log(f"writing {to_file}'s {report.lower()} "
              f"report to '{to_folder}'")


class StockPup(Analyst):

    def __init__(self, ticker=None, filename=None):
        self.ticker = ticker if ticker else self.get_ticker(filename)
        self.filename = filename if filename else self.get_filename(ticker)
        super().__init__(ticker=self.ticker, filename=self.filename)

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
        status = f"Setting Index to Years::"
        if not isinstance(dataframe.index, MultiIndex):
            return dataframe.set_index([
                Index(dataframe.index.year, name="YEAR"),
                Index(dataframe.index.quarter, name="QUARTER")
            ])
            self.success(status)
        else:
            self.error(status)
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
        self.log('Converting to Numeric DataTypes')
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

    def get_net_shares(self, dataframe):
        return dataframe['NET_SHARES']

    def fourth_quarter(self, x):
        return x.iat[0]

    def aggregate_quarters_to_years(self, dataframe):
        status = 'Aggregating quarters into years'
        return self.calculate_net_values(dataframe).groupby(level='YEAR').agg({
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
        })

    def calculate_moving_averages(self, dataframe, window=5):
        self.log(f'Calculating {window} year moving averages for Net Values')
        return (
            self.aggregate_quarters_to_years(dataframe)
                .rolling(window=window, min_periods=1)
                .median()
        )


class Edgar(Analyst):

    def __init__(self, ticker=None, filename=None):
        self.ticker = ticker if ticker else self.get_ticker(filename)
        self.filename = filename if filename else self.get_filename(ticker)
        super().__init__(ticker=self.ticker, filename=self.filename)

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
        status = f"Setting Index to Years::"
        try:
            dataframe.index = dataframe.index.year
            dataframe.index.name = 'YEAR'
        except AttributeError:
            self.error(status)
        else:
            self.success(status)
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
        self.log('Converting to Numeric DataTypes')
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

    def calculate_moving_averages(self, dataframe, window=5):
        self.log(f'Calculating {window} year moving averages for Net Values')
        return self.calculate_net_values(dataframe).rolling(window=window, min_periods=1).median()


class AssignSector(Analyst):
    """"""
    
    def __init__(self, ticker=None, filename=None,
                 to_folder=f"{ANALYSIS_FOLDER}{SECTORS_FOLDER}"):
        super().__init__(ticker=ticker, filename=filename, 
                         to_folder=to_folder, from_folder=SECTORS_FOLDER)
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

        self.symbols = pd.read_csv(
            self.filename, header=1,
            usecols=["Symbol", "Company Name"], index_col="Symbol"
        )
        self.symbols = self.symbols.rename(columns={
            "Symbol": "SYMBOL", "Company Name": "COMPANY"
        })
        self.symbols["SECTOR"] = self.sectors[self.ticker]

        self.write_report(
            dataframe=self.symbols, report="Sectors",
            to_file=self.ticker, to_folder=self.folder, 
        )


class AssignIndustry(Analyst):
    
    def __init__(self, ticker=None, filename=None,
                 to_folder=f"{ANALYSIS_FOLDER}{INDUSTRY_FOLDER}"):
        super().__init__(ticker=ticker, filename=filename, 
                         to_folder=to_folder, 
                         from_folder=INDUSTRY_FOLDER)
        self.folder = to_folder
        self.symbols = pd.read_csv(
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

        self.write_report(
            dataframe=self.symbols, report="Industry",
            to_file=self.ticker, to_folder=self.folder, 
        )