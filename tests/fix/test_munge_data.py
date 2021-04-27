import unittest

from haystack_munger import Munge
from haystack_utilities import (
    TEST_FOLDER, SECTORS_FOLDER, STOCKPUP_FOLDER, EDGAR_FOLDER,     
    PROCESSED_FOLDER, os, random_file
)


class TestMunger(unittest.TestCase):

    def test_munger_converts_none_to_zero_replacing_none_in_original_csv_to_zero_and_writes_a_newfile(self):
        folder_list = [SECTORS_FOLDER, STOCKPUP_FOLDER, EDGAR_FOLDER]
        for folder in folder_list:
            filename = Munge(
                random_file(from_folder=folder),
                to_folder=f"{TEST_FOLDER}{PROCESSED_FOLDER}{folder}"
            ).newfile
            with open(filename) as file:
                [self.assertNotIn("None", line) for line in file]
                [self.assertNotIn("2000-01-01", line) for line in file]
            self.assertTrue(os.path.exists(filename))



if __name__ == "__main__":
    unittest.main()