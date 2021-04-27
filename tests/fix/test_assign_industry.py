import unittest

from haystack_analyst import AssignIndustry
from haystack_utilities import (
    PROCESSED_FOLDER, INDUSTRY_FOLDER, TEST_FOLDER, ANALYSIS_FOLDER,
    random_ticker, os, pd
)

class TestAssignIndustries(unittest.TestCase):

    def setUp(self):
        self.ticker = random_ticker(
            from_folder=INDUSTRY_FOLDER,
        )
        self.industry = AssignIndustry(
            self.ticker,
            to_folder=(f'{TEST_FOLDER}{PROCESSED_FOLDER}'
                       f'{ANALYSIS_FOLDER}{INDUSTRY_FOLDER}')
        )
        self.symbols = self.industry.symbols

    def test_read_industry_and_symbols_returns_dataframe(self):
        self.assertIs(type(self.symbols), pd.DataFrame)

    def test_column_names(self):
        self.assertEqual(
            sorted(self.symbols.columns),
            ['COMPANY', "INDUSTRY", "SECTOR"]
        )
        
    def test_index_name_is_symbol(self):
        self.assertEqual(self.symbols.index.name, "Symbol")

    def test_data_has_unique_rows(self):
        self.assertFalse(self.symbols.index.duplicated().all())

    def test_industry_reports_are_written_to_industry_folder(self):
        self.assertTrue(os.path.exists(f"{self.industry.folder}/"
                                        f"{self.ticker}.CSV"))