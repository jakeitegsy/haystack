import unittest

from analyst import Analyst

class TestAnalyst(unittest.TestCase):
	
	def test_default_source_is_stockpup(self):
		self.assertEqual(Analyst().source, 'STOCKPUP')

	def test_default_discount_rate(self):
		self.assertEqual(Analyst().discount_rate, 0.0316)

	def test_ticker_symbol_when_ticker_provided(self):
		def ticker():
			return 'AAPL'
		self.assertEqual(ticker(), Analyst(ticker=ticker()).ticker)
	
	def test_none_has_no_length(self):
		with self.assertRaises(TypeError):
			len(None)