import datetime
import json
import yfinance as yahoo_finance

from scipy.stats import hmean
from haystack_utilities import (
    ANALYSIS_FOLDER, INDUSTRY_FOLDER, PROCESSED_FOLDER, 
    SECTORS_FOLDER,  STOCKPUP_FOLDER,
    pd, list_filetype, os, test_folder, makedir
)


class Analyst:

    def __init__(self, ticker=None, filename=None, to_folder=None,
                 from_folder=f"{PROCESSED_FOLDER}{STOCKPUP_FOLDER}", 
                 discount_rate=0.0316, source="STOCKPUP"):
        self.ticker, self.filename = self.get_ticker_and_filename(
            filename=filename,
            ticker=ticker,
            folder=from_folder,
        )
        self.rate = discount_rate
        self.source = source

    def aggregate(self, df):
        """returns dataframe with yearly summary for each column"""
        Q4 = lambda x: x[0]
        try: 
            df = df.groupby(level=0).agg({
                "CURRENT_ASSETS": Q4,
                "CURRENT_LIABILITIES": Q4,
                "NET_ASSETS": Q4,
                "NET_CASH": Q4,
                "NET_CASH_FIN": sum,
                "NET_CASH_INV": sum,
                "NET_CASH_OP": sum,
                "NET_DEBT": Q4,
                "NET_EXPENSES": sum,
                "NET_FCF": sum,
                "NET_FCF_SHY": sum,
                "NET_GOODWILL": Q4,
                "NET_INCOME": sum,
                "NET_INVESTED_CAP": Q4,
                "NET_LIABILITIES": Q4,
                "NET_NONCONTROLLING": Q4,
                "NET_REVENUE": sum,
                "NET_RETAINED": sum,
                "NET_EQUITY": Q4,
                "NET_SHARES": Q4,
                "NET_WORKING_CAP": Q4,
                "NET_TANGIBLE": Q4,
                "NET_DIVIDENDS": sum,
            })
        except TypeError as error_msg:
            write_error(self.filename, error_msg)
        except KeyError:
            pass

        return df

    def calc_diffs(self, data):
        assets = data["NET_ASSETS"]
        cash = data["NET_CASH"]
        current_assets = data["CURRENT_ASSETS"]
        debt = data["NET_DEBT"]
        equity = data["NET_EQUITY"]
        expenses = data["NET_EXPENSES"]
        fcf = data["NET_FCF"]
        fcf_shy = data["NET_FCF_SHY"]
        income = data["NET_INCOME"]
        invested_cap = data["NET_INVESTED_CAP"]
        liabilities = data["NET_LIABILITIES"]
        revenue = data["NET_REVENUE"]
        tangible = data["NET_TANGIBLE"]

        diffs_dict = {
            "DIFF_ASSETS_DEBT": assets - debt,
            "DIFF_ASSETS_LIABILITIES": assets - liabilities,
            "DIFF_CASH_DEBT": cash - debt,
            "DIFF_CASH_LIABILITIES": cash - liabilities,
            "DIFF_CURRENT_ASSETS_DEBT": current_assets - debt,
            "DIFF_CURRENT_ASSETS_LIABILITIES": (
                current_assets - liabilities
            ),
            "DIFF_EQUITY_DEBT": equity - debt,
            "DIFF_EQUITY_LIABILITIES": equity - liabilities,
            "DIFF_FCF_DEBT": fcf - debt,
            "DIFF_FCF_LIABILITIES": fcf - liabilities,
            "DIFF_FCF_SHY_DEBT": fcf_shy - debt,
            "DIFF_FCF_SHY_LIABILITIES": fcf_shy - liabilities,
            "DIFF_INCOME_DEBT": income - debt,
            "DIFF_INCOME_LIABILITIES": income - liabilities,
            "DIFF_TANGIBLE_DEBT": tangible - debt,
            "DIFF_TANGIBLE_LIABILITIES": tangible - liabilities,
        }

        return self.transform_dict(diffs_dict)

    def calc_dcf(self, values):
        return sum([(values[i] / ((1 + self.rate) ** i)) 
                    for i in range(len(values))])            

    def calc_growth(self, df, using="simple"):
        "Return Simple or Compound Growth Rate"
        if using == "compound":            
            try:
                avg = (((df.iloc[-1] / df.iloc[0]) 
                        ** (1 / (df.shape[0] - 1))) - 1)
            except ZeroDivisionError:
                avg = df.pct_change(axis=0)
        else:
            avg = df.pct_change(axis=0)

        avg = self.fix_nans(avg)
        avg = self.rename_columns(avg, prefix="GROWTH_")

        return avg

    def calc_net(self, data):
        net_dict = {
            "CURRENT_ASSETS": data["CURRENT_ASSETS"],
            "CURRENT_LIABILITIES": data["CURRENT_LIABILITIES"],
            "NET_ASSETS": data["NET_ASSETS"],
            "NET_CASH": data["NET_CASH"],
            "NET_CASH_FIN": -data["NET_CASH_FIN"],
            "NET_CASH_INV": -data["NET_CASH_INV"],
            "NET_CASH_OP": data["NET_CASH_OP"],
            "NET_DEBT": data["NET_DEBT"],
            "NET_EQUITY": data["NET_EQUITY"],
            "NET_EXPENSES": data["NET_REVENUE"] - data["NET_INCOME"],
            "NET_FCF_SHY": data["NET_CASH_OP"] + data["NET_CASH_INV"],
            "NET_GOODWILL": data["NET_GOODWILL"],
            "NET_INCOME": data["NET_INCOME"],
            "NET_LIABILITIES": data["NET_ASSETS"] - data["NET_EQUITY"],
            "NET_REVENUE": data["NET_REVENUE"],
            "NET_WORKING_CAP": (
                data["CURRENT_ASSETS"] - data["CURRENT_LIABILITIES"]
            ),
        }

        if self.source == "STOCKPUP":
            net_dict.update({
                "NET_EQUITY": data["NET_ASSETS"] - data["NET_LIABILITIES"],
                "NET_INVESTED_CAP": (
                     data["NET_DEBT"] + data["NET_PREFERRED"] + 
                     data["NET_NONCONTROLLING"] + data["NET_EQUITY"]
                 ),
                "NET_LIABILITIES": data["NET_LIABILITIES"],
                "NET_NONCONTROLLING": (
                    data["NET_NONCONTROLLING"] + data["NET_PREFERRED"]
                ),
                "NET_SHARES": data["NET_SHARES"],
                "NET_FCF": (data["NET_CASH_OP"] - 
                            data["CAPITAL_EXPENDITURES"]),  
                "NET_TANGIBLE": (
                    data["NET_ASSETS"] 
                 - (data["NET_LIABILITIES"] + data["NET_GOODWILL"])
                ),
            })

        if self.source == "EDGAR":
            net_dict.update({
                "NET_FCF": net_dict["NET_FCF_SHY"],
                "NET_INVESTED_CAP": (
                    data["NET_EQUITY"] + data["NET_DEBT"]
                ),
                "NET_LIABILITIES": data["NET_ASSETS"] - data["NET_EQUITY"],
                "NET_SHARES": (
                    data["NET_INCOME"] / data["PER_SHARE_EARNINGS_DILUTED"]
                ),
                "NET_TANGIBLE": (
                    data["NET_ASSETS"] 
                  - net_dict["NET_LIABILITIES"] + data["NET_GOODWILL"]
                ),
                "NET_NONCONTROLLING": 0.0,
            })
        
        net_dict["NET_DIVIDENDS"] = (
            data["PER_SHARE_DIVIDENDS"] * net_dict["NET_SHARES"]
        )
        net_dict["NET_RETAINED"] = (
            net_dict["NET_INCOME"] - net_dict["NET_DIVIDENDS"]
        )


        return self.transform_dict(net_dict)

    def calc_per_share(self, data):
        per_share = data.div(data.NET_SHARES, axis=0)
        per_share = self.rename_columns(per_share, prefix="PER_SHARE_")
        per_share["PER_SHARE_DCF_HISTORIC"] = (
            per_share["PER_SHARE_NET_FCF"]
            .expanding(min_periods=1)
            .apply(self.calc_dcf, raw=True)
        )
        per_share["PER_SHARE_DCF_SHY_HISTORIC"] = (
            per_share["PER_SHARE_NET_FCF_SHY"]
            .expanding(min_periods=1)
            .apply(self.calc_dcf, raw=True)
        )

        return per_share

    def calc_price_ratios(self, df):
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
                df[ratio] = df[numerator] / df["PER_SHARE_MARKET"]
            except ZeroDivisionError:
                df[ratio] = 0.0

    def calc_ratios(self, data):
        ratio = self.get_ratio
        assets = data["NET_ASSETS"]
        cash = data["NET_CASH"]
        current_assets = data["CURRENT_ASSETS"]
        debt = data["NET_DEBT"]
        equity = data["NET_EQUITY"]
        expenses = data["NET_EXPENSES"]
        fcf = data["NET_FCF"]
        fcf_shy = data["NET_FCF_SHY"]
        income = data["NET_INCOME"]
        invested_cap = data["NET_INVESTED_CAP"]
        liabilities = data["NET_LIABILITIES"]
        revenue = data["NET_REVENUE"]
        tangible = data["NET_TANGIBLE"]

        ratios_dict = {
            "RATIO_CASH_ASSETS": cash / assets,
            "RATIO_CASH_DEBT": ratio(cash, debt),
            "RATIO_CASH_LIABILITIES": cash / liabilities,
            "RATIO_CURRENT": ratio(
                current_assets, data["CURRENT_LIABILITIES"]
            ),
            "RATIO_CURRENT_ASSETS_LIABILITIES": ratio(
                current_assets, liabilities
            ),
            "RATIO_CURRENT_DEBT": ratio(current_assets, debt),
            "RATIO_CASH_TANGIBLE": ratio(cash, tangible),
            "RATIO_EQUITY_ASSETS": equity / assets,
            "RATIO_EQUITY_DEBT": ratio(equity, debt),
            "RATIO_EQUITY_LIABILITIES": equity / liabilities,
            "RATIO_EQUITY_TANGIBLE": ratio(equity, tangible),
            "RATIO_EXPENSES_REVENUE": ratio(expenses, revenue),
            "RATIO_FCF_ASSETS": fcf / assets,
            "RATIO_FCF_DEBT": ratio(fcf, debt),
            "RATIO_FCF_EQUITY": ratio(fcf, equity),
            "RATIO_FCF_EXPENSES": fcf / expenses,
            "RATIO_FCF_INVESTED_CAP": fcf / invested_cap,
            "RATIO_FCF_LIABILITIES": fcf / liabilities,
            "RATIO_FCF_TANGIBLE": ratio(fcf, tangible),
            "RATIO_FCF_SHY_ASSETS": fcf_shy / assets,
            "RATIO_FCF_SHY_DEBT": ratio(fcf_shy, debt),
            "RATIO_FCF_SHY_EQUITY": ratio(fcf_shy, equity),
            "RATIO_FCF_SHY_EXPENSES": fcf_shy / expenses,
            "RATIO_FCF_SHY_INVESTED_CAP": fcf_shy / invested_cap,
            "RATIO_FCF_SHY_LIABILITIES": fcf_shy / liabilities,
            "RATIO_FCF_SHY_TANGIBLE": ratio(fcf_shy, tangible),
            "RATIO_INCOME_ASSETS": income / assets,
            "RATIO_INCOME_DEBT": ratio(income, debt),
            "RATIO_INCOME_EQUITY": ratio(income, equity),
            "RATIO_INCOME_EXPENSES": income / expenses,
            "RATIO_INCOME_INVESTED_CAP": income / invested_cap,
            "RATIO_INCOME_LIABILITIES": income / liabilities,
            "RATIO_INCOME_TANGIBLE": ratio(income, tangible),
            "RATIO_TANGIBLE_ASSETS": tangible / assets,
            "RATIO_TANGIBLE_LIABILITIES": tangible / liabilities,
            "RATIO_TANGIBLE_DEBT": ratio(tangible, debt),
        }
        
        return self.transform_dict(ratios_dict)

    def combine_files(self, folder, ext="csv", header=None, 
                      axis=0, file_col="Symbol", ignore_index=False):
        "Returns dataframe of csv files in folder"
        file_col = file_col.upper()
        file_list = list_filetype(in_folder=folder, extension=ext)

        df_list = []
        try:
            for afile in file_list:
                try:
                    df = pd.read_csv(
                        f"{folder}{afile}", header=header, index_col=0,
                        squeeze=True
                    )
                    df[f"{file_col}"] = afile[:-4]
                    df_list.append(df)
                except FileNotFoundError:
                    pass
        except TypeError:
            pass

        combined = None
        try:
            combined = pd.concat(df_list, axis=axis,
                                ignore_index=ignore_index, sort=False)
        except ValueError:
            pass

        try:
            return combined.T.set_index(file_col)
        except (KeyError, AttributeError):
            return combined

    def fix_nans(self, df):
        fixed = df.replace([pd.np.inf, -pd.np.inf, pd.np.nan], 0)
        return fixed

    def fix_index(self, df):
        if self.source.upper() == "STOCKPUP":
            if type(df.index) is not pd.MultiIndex:
                df = df.set_index([pd.Index(df.index.year, name="YEAR"),
                                   pd.Index(df.index.quarter,
                                                        name="QUARTER")])

        if self.source.upper() == "EDGAR":
            try:
                df.index = df.index.year
                df.index.name = "YEAR"
            except AttributeError:
                return

        return df

    def yahoo_finance_download(self, tickers=None, start=None):
        if tickers is not None and start is not None:
            return yahoo_finance.download(
                list(tickers),
                start=start,
                end=datetime.date.today(),
                as_panel=False,
                threading=16
            )

    def get_current_prices(self, symbols="", df=None):
        """Return """
        try:
            tickers = df.index
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
            print("I could not download current prices, "
                  "using last known prices instead")
            last_known_prices = sorted(
                list_filetype(in_folder="prices", extension="pkl")
            )[-1]
            return pd.read_pickle(last_known_prices)

        return current_prices

    def get_ratio(self, numerator, denominator):
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

    def rename_columns(self, df, prefix=None):
        df = self.uppercase_labels(df)
        edgar = {
            "ASSETS": "NET_ASSETS",
            "CASH": "NET_CASH",
            "CASH_FLOW_FIN": "NET_CASH_FIN",
            "CASH_FLOW_INV": "NET_CASH_INV",
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
        stockpup = {
            "ASSETS": "NET_ASSETS",
            "BOOK VALUE OF EQUITY PER SHARE": "PER_SHARE_BOOK",
            "CASH AT END OF PERIOD": "NET_CASH", 
            "CASH FROM FINANCING ACTIVITIES": "NET_CASH_FIN",
            "CASH FROM INVESTING ACTIVITIES": "NET_CASH_INV",
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

        source = self.source.upper()
        if source == "STOCKPUP": source_columns = stockpup
        if source == "EDGAR": source_columns = edgar

        if prefix is not None:
            df = df.add_prefix(prefix)
        else:
            try:
                df = df.rename(columns=source_columns)
            except AttributeError:
                return

        return df[~df.index.duplicated()]

    def remove_incomplete_years(self, df):
        years = df.index.levels[0]

        for year in years:
            if not 3 < len(df.loc[year]) < 5:
                df = df.drop(year, axis=0, level=0)
        return df

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

    def score_price_ratios(self, df):
        price_ratios = pd.np.mean([
            df.RATIO_CASH_PRICE,
            df.RATIO_DCF_SHY_FORWARD_PRICE,
            df.RATIO_DCF_SHY_HISTORIC_PRICE,
            df.RATIO_EQUITY_PRICE,
            df.RATIO_INCOME_PRICE,
            df.RATIO_TANGIBLE_PRICE,
        ], axis=0)

        return self.transform(price_ratios)

    def transform_dict(self, data_dict):
        try:
            output = pd.DataFrame(data_dict)
        except ValueError:
            output = pd.Series(data_dict)
        except pd.core.indexes.base.InvalidIndexError:
            output = pd.DataFrame.from_dict(
                data_dict, orient='index', dtype=pd.np.float64
            ).T.drop_duplicates(keep='last')

        try:
            return output.sort_index()
        except TypeError:
            return output

    def uppercase_labels(self, df):
        try:
            data = df.rename(str.upper)
        except Exception:
            data = df.rename(str.upper, axis="columns")
        return data

    def write_report(self, df=None, report='', to_file=None, 
                     to_folder=None):
        makedir(to_folder)
        filename = f'{to_folder}{to_file}.csv'

        try:
            df.sort_index(axis=1).to_csv(filename, header=True)
        except (TypeError, ValueError):
            df.sort_index().to_csv(filename, header=True)
        except AttributeError:
            return "Invalid operation for NoneType"
        except FileNotFoundError:
            self.write_report(df=df, report=report,
                              to_file=to_file, to_folder=to_folder)
        
        print(f"writing {to_file}'s {report.lower()} "
              f"report to '{to_folder}'")


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
            df=self.symbols, report="Sectors",
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
            df=self.symbols, report="Industry",
            to_file=self.ticker, to_folder=self.folder, 
        )