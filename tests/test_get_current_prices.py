import unittest

from src.haystack_analyst import Analyst

@unittest.skip
class TestGetCurrentPrices(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None
        self.prices = Analyst().get_current_prices

    def test_get_current_prices_returns_dataframe_with_one_symbol(self):
        self.assertIs(
            type(self.prices(symbols="YUM")), pd.DataFrame
        )

    def test_get_current_prices_returns_dataframe_with_more_than_one_symbol(self):
        self.assertIs(
            type(self.prices(symbols=["AMZN" ,"GOOG"])),
            pd.DataFrame
        )



if __name__ == '__main__':
    unittest.main()