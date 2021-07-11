import unittest

from tests.test_analyze_stock_data import (
    TestData, pd, os, TEST_FOLDER, PROCESSED_FOLDER, ANALYSIS_FOLDER, 
    EDGAR_FOLDER
)


@unittest.skip
class TestOriginalTestData(TestData):

    def setUp(self):
        super().setUp()
        self.data = self.edgar.stock.data

    def test_original_data_contains_22_columns(self):
        self.assertEqual(len(self.data.columns), 22)
        self.assertEqual(
            sorted(self.data.columns),
            sorted(self.edgar.columns)
        )

    def test_index_of_data_is_fiscal_year(self):
        self.assertEqual(self.data.index.name, "fiscal_year")
        
    def test_index_of_data_is_datetime_index(self):
        self.assertIs(
            type(self.data.index), pd.DatetimeIndex
        )

    def test_data_has_unique_rows(self):
        self.assertFalse(self.data.index.duplicated().all())

@unittest.skip
class TestFixedIndices(TestData):

    def setUp(self):
        super().setUp()
        self.data = self.edgar.stock.fixed

    def test_fixed_index_is_year_only(self):
        self.assertIs(type(self.data.index), pd.Int64Index)

    def test_fixed_index_name_to_year(self):
        self.assertEqual(self.data.index.name, "YEAR")

    def test_data_has_unique_rows(self):
        self.assertFalse(self.data.index.duplicated().all())

@unittest.skip
class TestRenamedColumns(TestData):
    
    def setUp(self):
        super().setUp()
        self.renamed = self.edgar.stock.renamed

    def test_columns_are_unique(self):
        self.assertFalse(self.renamed.columns.duplicated().all())

    def test_data_has_unique_rows(self):
        self.assertFalse(self.renamed.index.duplicated().all())

    def test_renamed_columns_preserves_original_number_of_columns(self):
        self.assertEqual(
            len(self.renamed.columns),
            len(self.edgar.stock.data.columns)
        )

    def test_renamed_columns_changes_column_names_to_specific_list(self):
        self.assertTrue(all(self.renamed.columns.str.isupper()))
        self.assertEqual(
            sorted(self.renamed.columns),
            ['AMEND',
             'CAP_EX',
              'CURRENT_ASSETS',
              'CURRENT_LIABILITIES',
              'DOC_TYPE',
              'END_DATE',
              'NET_ASSETS',
              'NET_CASH',
              'NET_CASH_FIN',
              'NET_CASH_INV',
              'NET_CASH_OP',
              'NET_DEBT',
              'NET_EQUITY',
              'NET_GOODWILL',
              'NET_INCOME',
              'NET_REVENUE',
              'OP_INCOME',
              'PERIOD_FOCUS',
              'PER_SHARE_DIVIDENDS',
              'PER_SHARE_EARNINGS_BASIC',
              'PER_SHARE_EARNINGS_DILUTED',
              'SYMBOL']
        )

@unittest.skip
class TestDataDocType(TestData):

    def test_doc_type_of_quarterly_data_is_10_Q(self):
        self.assertEqual(self.edgar.stock.quarterly.DOC_TYPE.all(), '10-Q')

    def test_doc_type_of_annual_data_is_10_K(self):
        self.assertEqual(self.edgar.stock.annual.DOC_TYPE.all(), '10-K')


if __name__ == '__main__':
    unittest.main()