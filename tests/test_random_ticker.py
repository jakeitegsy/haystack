import unittest

from haystack_utilities import random_ticker, EDGAR_FOLDER, STOCKPUP_FOLDER


class TestRandomTicker(unittest.TestCase):

    def setUp(self):
        self.edgar_ticker = random_ticker(from_folder=EDGAR_FOLDER)
        self.stockpup_ticker = random_ticker(from_folder=STOCKPUP_FOLDER)

    def test_random_ticker_returns_a_random_ticker_from_a_folder_of_csvs(self):
        self.assertGreater(len(self.edgar_ticker), 0)
        self.assertGreater(len(self.stockpup_ticker), 0)
        self.assertIs(type(self.edgar_ticker), str)
        self.assertIs(type(self.stockpup_ticker), str)



if __name__ == '__main__':
    unittest.main()