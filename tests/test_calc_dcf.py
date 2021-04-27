import unittest
#import http

from haystack_analyst import Analyst
from haystack_utilities import pd
nan = pd.np.nan


class TestCalcDCF(unittest.TestCase):

    def setUp(self):
        self.calc_dcf = Analyst().calc_dcf

    def test_dcf_valuation_sums_each_cash_flow_using_discount_rate(self):
        values = [1, 1.5, 1.2, 1.1]
        self.assertEqual(
            round(self.calc_dcf(values),2), 4.58
        )

    def test_dcf_valuation_with_nan(self):
        values = [nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan,
                  0., 0., -0.47, -0.47, -0.235, -7.43670886, -4.23944783]
        pd.np.testing.assert_equal(
            round(self.calc_dcf(values), 2),
            nan
        )


if __name__ == '__main__':
    unittest.main()