import unittest
#import http

from haystack_analyst import Analyst
from haystack_utilities import os, pd, TEST_FOLDER


class TestWriteReports(unittest.TestCase):

    def setUp(self):
        self.writer = Analyst().write_report
        self.df = pd.DataFrame({"A": "Apple", "B": "Boy", "C": "Cat"}, 
                                index=['row'])
        self.to_folder = (f"{TEST_FOLDER}report/")
        self.to_file = "test_report"
        self.out_file = f"{self.to_folder}{self.to_file}.csv"

    def test_write_report_writes_file_to_folder(self):        
        self.writer(
            df=self.df,
            report="test",
            to_file=self.to_file,
            to_folder=self.to_folder,
        )
        self.assertTrue(os.path.exists(self.out_file))

    def test_write_report_returns_a_message_when_df_is_none(self):
        self.assertEqual(self.writer(), "Invalid operation for NoneType")


if __name__ == '__main__':
    unittest.main()