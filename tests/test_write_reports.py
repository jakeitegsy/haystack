import jadecobra.tester
import os
import pandas
import src.utilities

from src.utilities import testing_folder, write_report


class TestWriteReports(jadecobra.tester.TestCase):

    def test_write_report_writes_file_to_folder(self):
        src.utilities.write_report(
            dataframe=pandas.DataFrame(
                {"A": "Apple", "B": "Boy", "C": "Cat"},
                index=['row']
            ),
            report="test",
            to_file="test_report",
            to_folder=testing_folder('report'),
        )
        self.assertTrue(
            os.path.exists(testing_folder('report/test_report.csv'))
        )

    def test_write_report_returns_a_message_when_df_is_none(self):
        self.assertEqual(src.utilities.write_report(), "Invalid operation for NoneType")
        self.publish()