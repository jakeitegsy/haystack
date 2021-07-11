import unittest

from tests.test_analyze_stock_data import TestData, os


@unittest.skip
class TestSummaryReports(TestData):

    def setUp(self):
        super().setUp()

    def test_report_writes_summary_report_to_summary_folder(self):
        self.assertTrue(
            os.path.exists(
                f"{self.stockpup.to_folder}{self.stockpup.ticker}.csv"
            )
        )
        self.assertTrue(
            os.path.exists(
                f"{self.edgar.to_folder}{self.edgar.ticker}.csv"
            )
        )


