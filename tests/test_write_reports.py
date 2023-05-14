import jadecobra.tester
import os
import pandas
import src.utilities

from src.utilities import testing_folder, write_report


class TestWriteReports(jadecobra.tester.TestCase):

    def test_write_report_writes_file_to_folder(self):
        data_frame = pandas.DataFrame(
            {"A": "Apple", "B": "Boy", "C": "Cat"},
            index=['row']
        )
        src.utilities.write_report(
            data_frame=data_frame,
            report="test",
            to_file="test_report",
            to_folder=src.utilities.testing_folder('report'),
        )

        self.assertEqual(
            pandas.read_csv(testing_folder('report/test_report.csv')),
            data_frame
        )
        self.publish()

    def test_write_report_returns_a_message_when_df_is_none(self):
        self.assertEqual(src.utilities.write_report(), "Invalid operation for NoneType")