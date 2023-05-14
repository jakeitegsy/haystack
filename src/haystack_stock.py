from haystack_analyst import Analyst
from haystack_utilities import (
    PROCESSED_FOLDER, EDGAR_FOLDER, STOCKPUP_FOLDER, ANALYSIS_FOLDER,
    pd
)
from scipy.stats import hmean


class Stock:

    def __init__(self, ticker=None, filename=None, 
                 from_folder=f"{PROCESSED_FOLDER}{EDGAR_FOLDER}", 
                 to_folder=f"{PROCESSED_FOLDER}{ANALYSIS_FOLDER}",
                 source=None, 
                 index_col=None,
                 doc_type="10-K"):
        self.analyst = Analyst(
            ticker=ticker, filename=filename, to_folder=to_folder, 
            from_folder=from_folder, source=source
        )

        self.ticker = self.analyst.ticker
        self.to_folder = to_folder
        self.filename = self.analyst.filename

        try:
            self.data = pd.read_csv(
                self.filename,
                header=0,
                index_col=index_col,
                parse_dates=True,
                infer_datetime_format=True,
                engine="c",
            )
            if len(self.data) <= 1:
                self.data = None
        except (ValueError, pd.errors.EmptyDataError):
            self.data = None
        
        if self.data is not None:
            self.fixed = self.analyst.fix_index(self.data.copy())
            self.renamed = self.analyst.rename_columns(self.fixed)

            if source == "STOCKPUP":
                self.complete = self.analyst.remove_incomplete_years(
                    self.renamed
                )
                self.aggregate = self.analyst.calc_net(self.complete)
                self.net = self.analyst.aggregate(self.aggregate)

            if source == "EDGAR":
                self.quarterly = self.renamed[
                    self.renamed.DOC_TYPE == '10-Q'
                ]
                self.annual = self.renamed[
                    self.renamed.DOC_TYPE == '10-K'
                ]
                self.net = self.analyst.calc_net(self.annual)

            if len(self.net) <= 1:
                self.data = None
            else:
                # Moving Averages
                self.mavg = self.net.rolling(
                    window=5, min_periods=1
                ).mean()
                self.diffs = self.analyst.calc_diffs(self.mavg)
                self.growth = self.analyst.calc_growth(self.mavg)
                self.ratios = self.analyst.calc_ratios(self.mavg)
                self.sums = self.mavg.apply(sum)

                # PER SHARE
                self.per_share = self.analyst.calc_per_share(self.mavg)
                self.per_share_diffs = self.analyst.rename_columns(
                    self.diffs.div(self.net.NET_SHARES, axis=0), 
                    prefix="PER_SHARE_"
                )

                # Averages
                self.mavg_avg = self.mavg.iloc[-1]
                self.mavg_avg.NET_TANGIBLE = (
                    self.mavg_avg.NET_ASSETS
                 - (self.mavg_avg.NET_LIABILITIES + 
                    self.mavg_avg.NET_GOODWILL)
                )
                self.diffs_avg = self.analyst.calc_diffs(self.mavg_avg)
                self.ratios_avg = self.analyst.calc_ratios(self.sums)
                self.growth_avg = self.analyst.calc_growth(
                    self.mavg, using="compound"
                )

                # Per Share Averages
                self.per_share_diffs_avg = self.analyst.rename_columns(
                    (self.diffs_avg / self.mavg_avg.NET_SHARES), 
                    prefix="PER_SHARE_"
                )
                self.per_share_avg = self.per_share.iloc[-1]

                avg = self.ratios_avg
                diff = self.diffs_avg
                growth = self.growth_avg

                self.averages = pd.Series({
                    "AVERAGE_GROWTH": pd.np.mean([
                        growth.GROWTH_NET_ASSETS,
                        growth.GROWTH_NET_CASH,
                        growth.GROWTH_NET_CASH_INV,
                        growth.GROWTH_NET_EQUITY,
                        growth.GROWTH_NET_FCF,
                        growth.GROWTH_NET_FCF_SHY,
                        growth.GROWTH_NET_INVESTED_CAP,
                        growth.GROWTH_NET_REVENUE,
                        growth.GROWTH_NET_TANGIBLE,
                    ]),
                    "AVERAGE_RETURNS": pd.np.mean([
                        avg.RATIO_INCOME_ASSETS,
                        avg.RATIO_INCOME_EQUITY,
                        avg.RATIO_INCOME_EXPENSES,
                        avg.RATIO_INCOME_INVESTED_CAP,
                        avg.RATIO_INCOME_TANGIBLE,
                        avg.RATIO_FCF_ASSETS,
                        avg.RATIO_FCF_EQUITY,
                        avg.RATIO_FCF_EXPENSES,
                        avg.RATIO_FCF_INVESTED_CAP,
                        avg.RATIO_FCF_TANGIBLE,
                        avg.RATIO_FCF_SHY_ASSETS,
                        avg.RATIO_FCF_SHY_EQUITY,
                        avg.RATIO_FCF_SHY_EXPENSES,
                        avg.RATIO_FCF_SHY_INVESTED_CAP,
                        avg.RATIO_FCF_SHY_TANGIBLE,
                    ]),
                    "AVERAGE_SAFETY": pd.np.mean([
                        diff.DIFF_ASSETS_DEBT,
                        diff.DIFF_ASSETS_LIABILITIES,
                        diff.DIFF_CASH_DEBT,
                        diff.DIFF_CASH_LIABILITIES,
                        self.mavg_avg.NET_WORKING_CAP,
                        diff.DIFF_CURRENT_ASSETS_DEBT,
                        diff.DIFF_CURRENT_ASSETS_LIABILITIES,
                        diff.DIFF_EQUITY_DEBT,
                        diff.DIFF_EQUITY_LIABILITIES,
                        diff.DIFF_FCF_DEBT,
                        diff.DIFF_FCF_LIABILITIES,
                        diff.DIFF_FCF_SHY_DEBT,
                        diff.DIFF_FCF_SHY_LIABILITIES,
                        diff.DIFF_INCOME_DEBT,
                        diff.DIFF_INCOME_LIABILITIES,
                        diff.DIFF_TANGIBLE_DEBT,
                        diff.DIFF_TANGIBLE_LIABILITIES,
                    ]),
                    "DATA_END":  self.year_start_end(self.mavg, 
                                                     period="END"),
                    "DATA_START": self.year_start_end(self.mavg, 
                                                      period="START"),
                    "PER_SHARE_DCF_FORWARD": (
                        self.per_share_avg.PER_SHARE_NET_FCF / 
                        self.analyst.rate
                    ),
                    "PER_SHARE_DCF_SHY_FORWARD": (
                        self.per_share_avg.PER_SHARE_NET_FCF_SHY / 
                        self.analyst.rate
                    ),
                })

                self.summary = pd.concat([
                    self.averages,
                    self.diffs_avg,
                    self.growth_avg,
                    self.mavg_avg,
                    self.per_share_avg,
                    self.per_share_diffs_avg,
                    self.ratios_avg,
                ], axis=0)
            
                try:
                    del self.summary.GROWTH_NET_SHARES
                except Exception:
                    pass

                self.summary = self.analyst.fix_nans(self.summary)
                self.analyst.write_report(
                    df=self.summary, report="Summary",
                    to_file=self.ticker, to_folder=self.to_folder
                )

    def year_start_end(self, df, period="START"):
        if period.upper() == "START":
            data = df.index[0]
        if period.upper() == "END":
            data = df.index[-1]
        return data


class Stockpup(Stock):

    def __init__(self, ticker=None, filename=None, 
                 from_folder=f"{PROCESSED_FOLDER}{STOCKPUP_FOLDER}",
                 to_folder=None,
                 source="STOCKPUP",
                 index_col="Quarter end",
                 ):
        super().__init__(ticker=ticker, filename=filename, 
                     from_folder=from_folder, to_folder=to_folder, 
                     source=source, index_col=index_col)


class Edgar(Stock):

    def __init__(self, ticker=None, filename=None, 
                 from_folder=f"{PROCESSED_FOLDER}{EDGAR_FOLDER}",
                 to_folder=None,
                 source="EDGAR",
                 index_col="fiscal_year",
                 ):
        super().__init__(ticker=ticker, filename=filename,
                         from_folder=from_folder, to_folder=to_folder, 
                         source=source, index_col=index_col)
