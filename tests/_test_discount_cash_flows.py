import unittest
from haystack_analyst import Analyst
from numpy import nan
from numpy.testing import assert_equal


class TestGetDiscountCashFlowValue(unittest.TestCase):

    def test_dcf_valuation_sums_each_cash_flow_using_discount_rate(self):
        values = [1, 1.5, 1.2, 1.1]
        self.assertEqual(
            round(Analyst().get_discount_cash_flow_value(values),2), 4.58
        )

    def test_dcf_valuation_when_nan_in_record(self):
        values = [nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan,
                  0., 0., -0.47, -0.47, -0.235, -7.43670886, -4.23944783]
        assert_equal(
            round(Analyst().get_discount_cash_flow_value(values), 2),
            nan
        )


if __name__ == '__main__':
    unittest.main()