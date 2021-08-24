import unittest

from os import path
from pandas import DataFrame
from utilities import testing_folder, write_report


class TestWriteReports(unittest.TestCase):

    def test_write_report_writes_file_to_folder(self):        
        write_report(
            dataframe=DataFrame(
                {"A": "Apple", "B": "Boy", "C": "Cat"},
                index=['row']
            ),
            report="test",
            to_file="test_report",
            to_folder=testing_folder('report'),
        )
        self.assertTrue(
            path.exists(testing_folder('report/test_report.csv'))
        )

    def test_write_report_returns_a_message_when_df_is_none(self):
        self.assertEqual(write_report(), "Invalid operation for NoneType")


if __name__ == '__main__':
    unittest.main()