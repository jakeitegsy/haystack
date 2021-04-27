from tests.test_analyze_stock_data import (
    TestData, pd, os, TEST_FOLDER, PROCESSED_FOLDER, ANALYSIS_FOLDER, 
    STOCKPUP_FOLDER
)


class TestOriginalTestData(TestData):

    def setUp(self):
        super().setUp()
        self.data = self.stockpup.stock.data

    def test_original_data_contains_40_columns(self):
        self.assertEqual(len(self.data.columns), 40)
        self.assertEqual(
            sorted(self.data.columns),
            sorted(self.stockpup.columns)  
        )

    def test_index_of_data_is_Quarter_end(self):
        self.assertEqual(self.data.index.name, "Quarter end")

    def test_index_of_data_is_datetime_index(self):
        self.assertIs(
            type(self.data.index), pd.DatetimeIndex
        )

    def test_data_has_unique_rows(self):
        self.assertFalse(self.data.index.duplicated().all())


class TestFixedIndices(TestData):

    def setUp(self):
        super().setUp()
        self.data = self.stockpup.stock.fixed

    def test_symbol_is_multi_index_by_year_and_period(self):
        self.assertIs(
            type(self.data.index), pd.core.indexes.multi.MultiIndex
        )

    def test_names_of_index_are_year_and_period(self):
        self.assertEqual(['YEAR', 'QUARTER'], self.data.index.names)

    def test_data_has_unique_rows(self):
        self.assertFalse(self.data.index.duplicated().all())


class TestRenamedColumns(TestData):
    
    def setUp(self):
        super().setUp()
        self.renamed = self.stockpup.stock.renamed

    def test_columns_are_unique(self):
        self.assertFalse(self.renamed.columns.duplicated().all())

    def test_data_has_unique_rows(self):
        self.assertFalse(self.renamed.index.duplicated().all())

    def test_renamed_columns_preserves_original_number_of_columns(self):
        self.assertEqual(
            len(self.renamed.columns),
            len(self.stockpup.stock.data.columns)
        )

    def test_renamed_columns_changes_column_names_to_specific_list(self):
        self.assertTrue(all(self.renamed.columns.str.isupper()))
        self.assertEqual(
            sorted(self.renamed.columns),
            ['ASSET TURNOVER',
             'CAPITAL_EXPENDITURES',
             'CASH CHANGE DURING PERIOD',
             'CURRENT RATIO',
             'CURRENT_ASSETS',
             'CURRENT_LIABILITIES',
             'EARNINGS',
             'EQUITY TO ASSETS RATIO',
             'FREE CASH FLOW PER SHARE',
             'LONG-TERM DEBT TO EQUITY RATIO',
             'NET MARGIN',
             'NET_ASSETS',
             'NET_CASH',
             'NET_CASH_FIN',
             'NET_CASH_INV',
             'NET_CASH_OP',
             'NET_DEBT',
             'NET_EQUITY',
             'NET_GOODWILL',
             'NET_INCOME',
             'NET_LIABILITIES',
             'NET_NONCONTROLLING',
             'NET_PREFERRED',
             'NET_REVENUE',
             'NET_SHARES',
             'NET_SHARES_NO_SPLIT',
             'P/B RATIO',
             'P/E RATIO',
             'PER_SHARE_BOOK',
             'PER_SHARE_CUM_DIVIDENDS',
             'PER_SHARE_DIVIDENDS',
             'PER_SHARE_EARNINGS_BASIC',
             'PER_SHARE_EARNINGS_DILUTED',
             'PRICE',
             'PRICE HIGH',
             'PRICE LOW',
             'RATIO_DIVIDENDS',
             'ROA',
             'ROE',
             'SPLIT FACTOR']
        )


class TestCompleteYears(TestData):

    def test_processed_data_contains_complete_years(self):
        self.assertEqual(  
            len(self.stockpup.stock.complete.index.levels[1]), 4
        )


class TestAggregateData(TestData):

    def test_aggregate_data_returns_dataframe_with_23_columns(self):
        self.assertEqual(len(self.stockpup.stock.aggregate.columns), 23)

    def test_aggregate_data_columns_are_the_same_as_net_columns(self):
        self.assertEqual(
            sorted(self.stockpup.stock.aggregate.columns),
            sorted(self.net_columns)
        )
