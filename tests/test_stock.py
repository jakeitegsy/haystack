import jadecobra.tester
import src.stock


class TestAnalyst(jadecobra.tester.TestCase):

    def test_default_source_is_stockpup(self):
        self.assertEqual(src.stock.Stock().source, 'STOCKPUP')

    def test_default_discount_rate(self):
        self.assertEqual(src.stock.Stock().discount_rate, 0.0316)

    def test_ticker_symbol_when_ticker_provided(self):
        def ticker():
            return 'AAPL'
        self.assertEqual(ticker(), src.stock.Stock(ticker=ticker()).ticker)

    def test_none_has_no_length(self):
        with self.assertRaises(TypeError):
            len(None)