from stock import Stock
from pandas import concat
from numpy import median
from utilities import (
    analysis_folder, industry_folder, processed_folder
)


class Industries:

    def __init__(self, 
                 industry_folder=None, stocks_folder=None,
                 to_folder=processed_folder(analysis_folder(industry_folder())),
                 ):
        analyst = Stock(from_folder=industry_folder,
                          to_folder=to_folder)
        
        self.industries_df = analyst.combine_files(
            industry_folder, file_col="SECTOR", header=0
        )
        
        self.stocks_df = analyst.combine_files(
            stocks_folder, axis=1, ignore_index=True
        )


        if self.stocks_df is not None \
        and self.industries_df is not None:
            self.symbols = concat(
                [self.industries_df, self.stocks_df], axis=1,
            )
            self.symbols["PER_SHARE_MARKET"] = (
                analyst.get_current_prices(df=self.symbols)
            )

            # Analysis
            analyst.calc_price_ratios(self.symbols)
            self.symbols["AVERAGE_PRICE_RATIOS"] = median([
                self.symbols.RATIO_CASH_PRICE,
                self.symbols.RATIO_DCF_SHY_FORWARD_PRICE,
                self.symbols.RATIO_DCF_SHY_HISTORIC_PRICE,
                self.symbols.RATIO_EQUITY_PRICE,
                self.symbols.RATIO_FCF_PRICE,
                self.symbols.RATIO_FCF_SHY_PRICE,
                self.symbols.RATIO_INCOME_PRICE,
                self.symbols.RATIO_TANGIBLE_PRICE,
            ], axis=0)

            self.industries = analyst.fix_nans(self.symbols)

            # name a sector for stock that does not fall into the NASDAQ industry anymore
            self.industries['SECTOR'] = (
                self.industries['SECTOR'].replace(0, 'unknown')
            )

            self.reports = self.industries.groupby(['SECTOR'])
            [analyst.write_report(
                    df=self.reports.get_group(group),
                    report='Industries',
                    to_file=group,
                    to_folder=to_folder,
                )
                for group in self.reports.groups.keys()]