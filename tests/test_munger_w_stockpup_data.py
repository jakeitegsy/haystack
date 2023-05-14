import jadecobra.tester
import pandas
import src.stock
import src.munger


class TestMungerWithStockPupData(jadecobra.tester.TestCase):

    stockpup = src.stock.Stock(source='STOCKPUP', ticker='BAC').get_stock()

    def munger(self):
        return src.munger.Munger(
            ticker='BAC',
            raw_data=self.stockpup.get_raw_data(),
            filename=self.stockpup.filename,
            mappings=self.stockpup.columns_mapping()
        )

    def test_convert_2000_new_year_to_1999_year_end_for_stockpup(self):
        try:
            self.assertFalse(
                self.munger().convert_2000_new_year_to_1999_year_end(
                    self.stockpup.get_raw_data()
                ).loc['2000-01-01'].values.any()
            )
        except KeyError:
            return

    def test_set_uppercase_column_names(self):
        pandas.testing.assert_index_equal(
            (
                self.munger()
                    .set_uppercase_column_names(
                        self.stockpup.get_raw_data()
                    ).columns
            ),
            (
                self.stockpup.get_raw_data()
                    .rename(str.upper, axis='columns')
                    .columns
            )
        )

    def test_rename_columns(self):
        pandas.testing.assert_index_equal(
            (
                self.munger()
                    .rename_columns(
                        dataframe=self.stockpup.get_raw_data(),
                        mappings=self.stockpup.columns_mapping()
                    )
                    .columns
            ),
            pandas.Index([
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
        self.publish()