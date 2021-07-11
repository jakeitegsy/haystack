import unittest
#import http

from haystack_analyst import Analyst
from haystack_utilities import os, pd, janitor, TEST_FOLDER, STOCKPUP_FOLDER

@unittest.skip
class TestCombineCSV(unittest.TestCase):

    def setUp(self):
        self.combine = Analyst().combine_files

    def test_combine_csv_returns_None_for_invalid_folders(self):
        combined = self.combine(folder="bob")
        self.assertIs(combined, None)

    def test_combine_csv_returns_a_dataframe_when_folder_contains_csv_with_multiple_columns(self):
        combined = self.combine(folder=STOCKPUP_FOLDER, 
                                file_col="SECTOR")
        self.assertIs(type(combined), pd.DataFrame)

    def test_combine_csv_returns_a_dataframe_when_folder_contains_csv_with_one_column(self):
        combined = self.combine(folder="processed/analysis/stockpup_data/")
        self.assertIs(type(combined), pd.Series)


if __name__ == '__main__':
    unittest.main()