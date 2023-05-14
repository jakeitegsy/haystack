from pandas import concat
from numpy import median
from stock import Stock
from utilities import (
    processed_folder, sectors_folder, analysis_folder
)



class Sectors:

    def __init__(
        self, sectors_folder=None, stocks_folder=None,
        to_folder=processed_folder(analysis_folder(sectors_folder()))
    ):
        analyst = Stock(
            from_folder=sectors_folder, to_folder=to_folder
        )
        
        self.sectors_df = analyst.combine_files(
            sectors_folder, file_col="SECTOR", header=0
        )
        
        self.stocks_df = analyst.combine_files(
            stocks_folder, axis=1, ignore_index=True
        )

        if self.stocks_df is not None and self.sectors_df is not None:
            self.symbols = concat(
                [self.sectors_df, self.stocks_df], axis=1, sort=True,
            )
            self.symbols["PER_SHARE_MARKET"] = analyst.get_current_prices(
                df=self.symbols
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

            self.sectors = analyst.fix_nans(self.symbols)

            # name a sector for stock that does not fall into the SPDRS sectors anymore
            self.sectors['SECTOR_SYMBOL'] = (
                self.sectors['SECTOR_SYMBOL'].replace(0, 'XL0')
            )

            self.reports = self.sectors.groupby(['SECTOR_SYMBOL'])
            [analyst.write_report(
                    df=self.reports.get_group(group),
                    report='Sectors',
                    to_file=group,
                    to_folder=to_folder,
                )
                for group in self.reports.groups.keys()]
