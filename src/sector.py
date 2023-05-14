from stock import Stock
from pandas import read_csv
from utilities import (
    processed_folder, sectors_folder, analysis_folder, makedir
)


class Sector:

    def __init__(
        self, ticker=None, filename=None,
        from_folder=processed_folder(analysis_folder(sectors_folder())),
        to_folder=processed_folder(sectors_folder('score_total_reports/'))
    ):

        if ticker is not None or filename is not None:
            analyst = Stock(ticker=ticker, filename=filename,
                              from_folder=from_folder)
            self.ticker = analyst.ticker
            self.folder = f'{to_folder}{self.ticker}'
            makedir(self.folder)
            try:
                self.data = read_csv(analyst.filename, 
                                          header=0,
                                          index_col=0,
                                          engine='c')
            except (Exception, ValueError):
                self.data = None

            if self.data is not None:
                self.symbol = self.data.copy()
                df = self.symbol
                df['SCORE_GROWTH'] = (
                    df.AVERAGE_GROWTH / max(df.AVERAGE_GROWTH)
                )
                df['SCORE_PRICE'] = (
                    df.AVERAGE_PRICE_RATIOS/ max(df.AVERAGE_PRICE_RATIOS)
                )
                df["SCORE_RETURNS"] = (
                    df.AVERAGE_RETURNS / max(df.AVERAGE_RETURNS)
                )

                df['SCORE_SAFETY'] = (
                    df.AVERAGE_SAFETY / max(df.AVERAGE_SAFETY)
                )
                df['SCORE_TOTAL'] = analyst.score(dataframe=df)
                df = analyst.fix_nans(df)
                self.top_x_positions(self.symbol)
                print(self.ticker)
        else:
            self.data = None

    def top_x_positions(self, dataframe, positions=None, margin=0.04,
                        category='SCORE_TOTAL'):
        judge = Stock().judge_growth
        dataframe = dataframe.sort_values(by=[category], ascending=False)
        summary = dataframe.copy()
        summary.columns = [x.replace("_", "\n") for x in summary.columns]

        Stock().write_report(
            df=summary,
            report="SECTOR_SUMMARY",
            to_file=self.ticker,
            to_folder=f"{self.folder}/"
        )

        try:
            top_x = dataframe[:positions]
        except IndexError:
            top_x = dataframe[len(dataframe)]
        

        for i in range(len(top_x.index)):
            rank = i+1
            df = top_x.iloc[i]
            prose = f"""CAVEAT
The analysis below was written using 5 year moving average calculations on data available from www.stockpup.com or from the EDGAR database at www.sec.gov for {str(df.COMPANY)}({df.name}) in the {str(df.SECTOR_SYMBOL)}({str(df.SECTOR_NAME)}) Sector covering the years {int(df.DATA_START)} to {int(df.DATA_END)}. The average number of shares used in per share calculations was {int(df.NET_SHARES):,}.


                                SAFETY OF PRINCIPAL
                            "RULE #1: Do not Lose Money"
                      "An investment operation is one which,
                              upon thorough analysis,
                           promises safety of principal"



BALANCE SHEET
Current Assets were {int(df.CURRENT_ASSETS):,} and Current Liabilities were {int(df.CURRENT_LIABILITIES):,} with average Working Capital of {int(df.NET_WORKING_CAP):,}. Average Assets were ${int(df.NET_ASSETS):,}, Cash Assets were ${int(df.NET_CASH):,}, Intangible Assets and Goodwill were ${int(df.NET_GOODWILL):,}, while Net Tangible Assets were ${int(df.NET_TANGIBLE):,}, Liabilities were ${int(df.NET_LIABILITIES):,} with Debt of ${int(df.NET_DEBT):,}. Common Shareholder Equity was ${int(df.NET_EQUITY):,}, and Non-Controlling Interests were ${int(df.NET_NONCONTROLLING):,} giving Invested Capital of ${int(df.NET_INVESTED_CAP):,}.

These figures translate to Current Assets of ${df.PER_SHARE_CURRENT_ASSETS:.2f} per share, Current Liabilities of ${df.PER_SHARE_CURRENT_LIABILITIES:.2f} per share, Working Capital of ${df.PER_SHARE_NET_WORKING_CAP:.2f} per share, Assets of ${df.PER_SHARE_NET_ASSETS:.2f} per share, Cash Assets of ${df.PER_SHARE_NET_CASH:.2f} per share, Intangible Assets and Goodwill of ${df.PER_SHARE_NET_GOODWILL:.2f} per share, Tangible Assets of ${df.PER_SHARE_NET_TANGIBLE:.2f} per share, Liabilities of ${df.PER_SHARE_NET_LIABILITIES:.2f} per share, Debt of ${df.PER_SHARE_NET_DEBT:.2f} per share, Common Shareholder Equity of ${df.PER_SHARE_NET_EQUITY:.2f} per share, Non-Controlling Interests of ${df.PER_SHARE_NET_NONCONTROLLING:.2f} per share, with Net Invested Capital of ${df.PER_SHARE_NET_INVESTED_CAP:.2f} per share.

This company had a difference of ${int(df.DIFF_CURRENT_ASSETS_LIABILITIES):,} between Current Assets and Total Liabilities, ${int(df.DIFF_ASSETS_LIABILITIES):,} between Assets and Liabilities, ${int(df.DIFF_CASH_LIABILITIES):,} between Cash and Liabilities, ${int(df.DIFF_TANGIBLE_LIABILITIES):,} between Tangible Assets and Liabilities, and between Equity and Liabilities was ${int(df.DIFF_EQUITY_LIABILITIES):,}. The gap between Current Assets and Debt was ${int(df.DIFF_CURRENT_ASSETS_DEBT):,}, between Assets and Debt was ${int(df.DIFF_ASSETS_DEBT):,}, between Cash and Debt was ${int(df.DIFF_CASH_DEBT):,}, between Tangible Assets and Debt ${int(df.DIFF_TANGIBLE_DEBT):,} and ${int(df.DIFF_EQUITY_DEBT):,} between Equity and Debt.

The difference between Income and Liabilities was ${int(df.DIFF_INCOME_LIABILITIES):,}, ${int(df.DIFF_FCF_LIABILITIES):,} between Free Cash Flow and Liabilities, and ${int(df.DIFF_FCF_SHY_LIABILITIES):,} between Shy Free Cash Flow and Liabilities. It was ${int(df.DIFF_INCOME_DEBT):,} between Income and Debt, ${int(df.DIFF_FCF_DEBT):,} between Free Cash Flow and Debt, and ${int(df.DIFF_FCF_SHY_DEBT):,} between Shy Free Cash Flow and Debt.

On a per share basis the difference between Current Assets and Total Liabilities was ${df.PER_SHARE_DIFF_CURRENT_ASSETS_LIABILITIES:.2f} per share, ${df.PER_SHARE_DIFF_ASSETS_LIABILITIES:.2f} per share between Assets and Liabilities, ${df.PER_SHARE_DIFF_CASH_LIABILITIES:.2f} per share between Cash and Liabilities, ${df.PER_SHARE_DIFF_TANGIBLE_LIABILITIES:.2f} per share between Tangible Assets and Liabilities, and ${df.PER_SHARE_DIFF_EQUITY_LIABILITIES:.2f} per share between Common Shareholder Equity and Liabilities. The gap between Current Assets and Debt was ${df.PER_SHARE_DIFF_CURRENT_ASSETS_DEBT:.2f} per share, ${df.PER_SHARE_DIFF_ASSETS_DEBT:.2f} per share between Assets and Debt, ${df.PER_SHARE_DIFF_CASH_DEBT:.2f} per share between Cash and Debt, ${df.PER_SHARE_DIFF_TANGIBLE_DEBT:.2f} per share between Tangible Assets and Debt, and ${df.PER_SHARE_DIFF_EQUITY_DEBT:.2f} per share between Common Shareholder Equity and Debt.

The difference between Income and Liabilities was ${df.PER_SHARE_DIFF_INCOME_LIABILITIES:.2f} per share, ${df.PER_SHARE_DIFF_FCF_LIABILITIES:.2f} per share between Free Cash Flow and Liabilities, and ${df.PER_SHARE_DIFF_FCF_SHY_LIABILITIES:.2f} per share between Shy Free Cash Flow and Liabilities. It was ${df.PER_SHARE_DIFF_INCOME_DEBT:.2f} per share between Net Income and Debt, ${df.PER_SHARE_DIFF_FCF_DEBT:.2f} per share between Free Cash Flow and Debt, and ${df.PER_SHARE_DIFF_FCF_SHY_DEBT:.2f} per share between Shy Free Cash Flow and Debt.

This means that Current Assets were {df.RATIO_CURRENT:.2f} times Current Liabilities, {df.RATIO_CURRENT_ASSETS_LIABILITIES:.2f} times Total Liabilities and {df.RATIO_CURRENT_DEBT:.2f} times Debt. Cash Assets were {df.RATIO_CASH_ASSETS:.2f} times Total Assets, {df.RATIO_CASH_TANGIBLE:.2f} times Net Tangible Assets, {df.RATIO_CASH_LIABILITIES:.2f} times Total Liabilities and {df.RATIO_CASH_DEBT:.2f} times Debt. Net Tangible Assets were {df.RATIO_TANGIBLE_ASSETS:.2f} times Total Assets, {df.RATIO_TANGIBLE_LIABILITIES:.2f} times Total Liabilities, and {df.RATIO_TANGIBLE_DEBT:.2f} times Debt. Common Shareholder Equity was {df.RATIO_EQUITY_ASSETS:.2f} times Total Assets, {df.RATIO_EQUITY_TANGIBLE:.2f} time Net Tangible Assets, {df.RATIO_EQUITY_LIABILITIES:.2f} times Total Liabilities, and {df.RATIO_EQUITY_DEBT:.2f} times Debt. 

Net Income was {df.RATIO_INCOME_LIABILITIES:.2f} times Liabilities and {df.RATIO_INCOME_DEBT:.2f} times Debt. Free Cash Flow was {df.RATIO_FCF_LIABILITIES:.2f} times Liabilities and {df.RATIO_FCF_DEBT:.2f} times Debt. Shy Free Cash Flow was {df.RATIO_FCF_SHY_LIABILITIES:.2f} times Liabilities and {df.RATIO_FCF_SHY_DEBT:.2f} times Debt.

The Average Safety Score for {str(df.COMPANY)} using Assets, Cash, Current Assets, Equity, Shy Free Cash Flow, Income, Liabilities and Debt is ${df.AVERAGE_SAFETY:,.2f} or ${(df.AVERAGE_SAFETY/df.NET_SHARES):.2f} per share.


                                    RETURNS
                            "RULE #2: See Rule # 1"
                    "An investment operation is one which,
                           upon thorough analysis,
                         promises an adequate return."



INCOME STATEMENT
On average {df.name} had Revenue of ${int(df.NET_REVENUE):,} and Expenses of ${int(df.NET_EXPENSES):,} with Net Income available to the common shareholder of ${int(df.NET_INCOME):,}. Dividends paid out were ${int(df.NET_DIVIDENDS):,} with Retained Earnings of ${int(df.NET_RETAINED):,}. 

This translates to Revenue of ${df.PER_SHARE_NET_REVENUE:.2f} per share, Expenses of ${df.PER_SHARE_NET_EXPENSES:.2f} per share, Net Income of ${df.PER_SHARE_NET_INCOME:.2f} per share, Dividends paid out of ${df.PER_SHARE_NET_DIVIDENDS:.2f} per share and Retained Earnings of ${df.PER_SHARE_NET_RETAINED:.2f} per share.

The company spent ${df.RATIO_EXPENSES_REVENUE:.2f} on average to make $1.00 of Revenue, returning Net Income of ${df.RATIO_INCOME_ASSETS:.2f} on every dollar of Assets, ${df.RATIO_INCOME_TANGIBLE:.2f} on every dollar of Tangible Assets, ${df.RATIO_INCOME_EQUITY:.2f} on every dollar of Equity, ${df.RATIO_INCOME_INVESTED_CAP:.2f} on every dollar of Invested Capital and ${df.RATIO_INCOME_EXPENSES:.2f} for every dollar of Expenses.

CASH FLOW
Cash Flows Provided by (Used in) Financing Activities was ${int(df.NET_CASH_FIN):,}, Cash Flows Provided by (Used in) Investing Activities was ${int(df.NET_CASH_INV):,} and Cash Flows Provided by (Used in) Operating Activities was ${int(df.NET_CASH_OP):,}, giving Free Cash Flows of ${int(df.NET_FCF):,}. A conservative estimate of Free Cash Flows which is calculated by subtracting the amount of Cash Flows Provided by (Used in) Investing Activities from Operating Activities and referred to in this document as "Shy Free Cash Flow" was ${int(df.NET_FCF_SHY):,}.

On a per share basis these are Cash Flows Provided by (Used in) Financing Activities of ${df.PER_SHARE_NET_CASH_FIN:.2f} per share, Cash Flows Provided by (Used in) Investing Activities of ${df.PER_SHARE_NET_CASH_INV:.2f} per share, Cash Flows Provided by (Used in) Operating Activities of ${df.PER_SHARE_NET_CASH_OP:.2f} per share, Free Cash Flow of ${df.PER_SHARE_NET_FCF:.2f} per share, and Shy Free Cash Flow ${df.PER_SHARE_NET_FCF_SHY:.2f} per share.

{str(df.COMPANY)} had on average Free Cash Flow of ${df.RATIO_FCF_ASSETS:.2f} for every dollar of Assets, {df.RATIO_FCF_TANGIBLE:.2f} for every dollar of Tangible Assets, ${df.RATIO_FCF_EQUITY:.2f} for every dollar of Equity,
${df.RATIO_FCF_INVESTED_CAP:.2f} for every dollar of Invested Capital, and ${df.RATIO_FCF_EXPENSES:.2f} for every dollar of Expenses. On average the company had Shy Free Cash Flow of ${df.RATIO_FCF_SHY_ASSETS:.2f} per dollar of Assets, ${df.RATIO_FCF_SHY_TANGIBLE:.2f} per dollar of Tangible Assets, ${df.RATIO_FCF_SHY_EQUITY:.2f} per dollar of Equity, ${df.RATIO_FCF_SHY_INVESTED_CAP:.2f} per dollar of Invested Capital,
and ${df.RATIO_FCF_SHY_EXPENSES:.2f} for every dollar of Expenses.

The average returns for {df.name} using Income, Free Cash Flow, Shy Free Cash Flow, Assets, Equity, Expenses, Invested Capital and Tangible Assets was ${df.AVERAGE_RETURNS:.2f} per dollar.


                                    GROWTH
                        "RULE# 3: Past performance 
                is not an accurate indicator of future growth"
                    "it tells you what has happened so far
                           and gives you an idea of where 
                               things are going"


BALANCE SHEET
Current Assets {judge(df.GROWTH_CURRENT_ASSETS)} at a rate of {df.GROWTH_CURRENT_ASSETS:.4f}, while Current Liabilities  {judge(df.GROWTH_CURRENT_LIABILITIES)} at a rate of {df.GROWTH_CURRENT_LIABILITIES:.4f}, Working Capital {judge(df.GROWTH_NET_WORKING_CAP)} at a rate of {df.GROWTH_NET_WORKING_CAP:.4f}, Total Assets {judge(df.GROWTH_NET_ASSETS)} at a rate of {df.GROWTH_NET_ASSETS:.4f}, Cash Assets {judge(df.GROWTH_NET_CASH)} at a rate of {df.GROWTH_NET_CASH:.4f}, Intangible Assets and Goodwill {judge(df.GROWTH_NET_GOODWILL)} at a rate of {df.GROWTH_NET_GOODWILL:.4f}. Net Tangible Assets {judge(df.GROWTH_NET_TANGIBLE)} at a speed of {df.GROWTH_NET_TANGIBLE:.4f}, Total Liabilities {judge(df.GROWTH_NET_LIABILITIES)} at a speed of {df.GROWTH_NET_LIABILITIES:.4f}, Debt {judge(df.GROWTH_NET_DEBT)} at a speed of {df.GROWTH_NET_DEBT:.4f}, Common Shareholder Equity {judge(df.GROWTH_NET_EQUITY)} at a speed of {df.GROWTH_NET_EQUITY:.4f}, Non-Controlling Interests {judge(df.GROWTH_NET_NONCONTROLLING)} at a speed of {df.GROWTH_NET_NONCONTROLLING:.4f}, Invested Capital {judge(df.GROWTH_NET_INVESTED_CAP)} at a speed of {df.GROWTH_NET_INVESTED_CAP:.4f}. The average number of outstanding shares {judge(df.GROWTH_NET_SHARES)} at a speed of {df.GROWTH_NET_SHARES:.4f}.

INCOME STATEMENT
Revenue {judge(df.GROWTH_NET_REVENUE)} at a pace of {df.GROWTH_NET_REVENUE:.4f}, Expenses {judge(df.GROWTH_NET_EXPENSES)} at a pace of {df.GROWTH_NET_EXPENSES:.4f}, Net Income {judge(df.GROWTH_NET_INCOME)} at a pace of {df.GROWTH_NET_INCOME:.4f}, Dividends paid out {judge(df.GROWTH_NET_DIVIDENDS)} at a pace of {df.GROWTH_NET_DIVIDENDS:.4f} and Retained Earnings {judge(df.GROWTH_NET_RETAINED)} at a pace of {df.GROWTH_NET_RETAINED:.4f}.

CASH FLOWS
Net Cash Provided by (Used in) Financing Activities {judge(df.GROWTH_NET_CASH_FIN)} at a frequency of {df.GROWTH_NET_CASH_FIN:.4f}, Cash Provided by (Used in) Investing Activities {judge(df.GROWTH_NET_CASH_INV)} at a frequency of {df.GROWTH_NET_CASH_INV:.4f}, Cash Provided by (Used in) Operating Activities {judge(df.GROWTH_NET_CASH_OP)} at a frequency of {df.GROWTH_NET_CASH_OP:.4f}, Free Cash Flow {judge(df.GROWTH_NET_FCF)} at a frequency of {df.GROWTH_NET_FCF:.4f} and Shy Free Cash Flow {judge(df.GROWTH_NET_FCF_SHY)} at a frequency of {df.GROWTH_NET_FCF_SHY:.4f}.

The Average Growth rate for {df.name} using Cash, Net Cash used in Investing, Equity, Shy Free Cash Flow, Invested Capital, Revenue, Net Tangible Assets and Working Capital was {df.AVERAGE_GROWTH:.4f}.


                                MARGIN OF SAFETY
                        "RULE #4: Price is what you pay,
                             Value is what you get"
                     "The Margin of Safety is always dependent 
                 on the price paid. It will be large at one price, 
                 small at some higher price, non-existent at some 
                              still higher price"


PRICE vs VALUE
The last trade price for {df.name} at the writing of this report was ${df.PER_SHARE_MARKET:.2f} per share, giving Net Income, Free Cash Flow and Shy Free Cash Flow yields of ${df.RATIO_INCOME_PRICE:.2f}, ${df.RATIO_FCF_PRICE:.2f} and ${df.RATIO_FCF_SHY_PRICE:.2f} respectively. For the Net Income, Free Cash Flow and Shy Free Cash Flow yield to be $({margin}), the stock would have to trade at prices of ${(df.PER_SHARE_NET_INCOME/margin):.2f}, ${(df.PER_SHARE_NET_FCF/margin):.2f} and ${(df.PER_SHARE_NET_FCF_SHY/margin):.2f} respectively. To give yields that match the Average Returns of ${df.AVERAGE_RETURNS:.2f}, the market price for one share would have to be ${(df.PER_SHARE_NET_INCOME/df.AVERAGE_RETURNS):.2f}, ${(df.PER_SHARE_NET_FCF/df.AVERAGE_RETURNS):.2f} and ${(df.PER_SHARE_NET_FCF_SHY/df.AVERAGE_RETURNS):.2f} respectively.

Historic Discounted Cash Flow for {str(df.COMPANY)} was ${df.PER_SHARE_DCF_HISTORIC:.2f} per share while Shy Historic Discounted Cash Flow ${df.PER_SHARE_DCF_SHY_HISTORIC:.2f} per share. Discounted Cash Flow is ${df.PER_SHARE_DCF_FORWARD:.2f} per share and Shy Discounted Cash Flow is ${df.PER_SHARE_DCF_SHY_FORWARD:.2f} per share.

By comparison Cash Assets are {df.RATIO_CASH_PRICE:.2f} times the market price, Net Tangible Assets are {df.RATIO_TANGIBLE_PRICE:.2f} times the market price, Equity is {df.RATIO_EQUITY_PRICE:.2f} times the market price,Historic Discounted Cash Flow is {df.RATIO_DCF_HISTORIC_PRICE:.2f} times the market price, Shy Historic Discounted Cash Flow is {df.RATIO_DCF_SHY_HISTORIC_PRICE:.2f} times the market price, Discounted Cash Flow is {df.RATIO_DCF_FORWARD_PRICE:.2f} times the market price and Shy Discounted Cash Flow is {df.RATIO_DCF_SHY_FORWARD_PRICE:.2f} times the market price. The average of these Valuations to Price Ratios is {df.AVERAGE_PRICE_RATIOS:.4f}.

SCORE
{str(df.COMPANY)} ranks Number {rank} on the {category.replace('_', ' ')} rankings in the {str(df.SECTOR_SYMBOL)} sector with a Growth Score of {df.SCORE_GROWTH:.4f}, Price Score of {df.SCORE_PRICE:.4f}, Returns Score of {df.SCORE_RETURNS:.6f} and Safety Score of {df.SCORE_SAFETY:.4f} for a Total Score of {df.SCORE_TOTAL:.4f}."""
            with open(f"{self.folder}/{rank}_{df.name}.txt", 'w') as outfile:
                        outfile.write(f'{prose}')