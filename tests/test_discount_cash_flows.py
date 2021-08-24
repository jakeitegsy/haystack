import unittest
from stock import get_discount_cash_flow_value
from numpy import nan
from numpy.testing import assert_equal


class TestGetDiscountCashFlowValue(unittest.TestCase):

    def test_dcf_valuation_sums_each_cash_flow_using_discount_rate(self):
        self.assertEqual(
            round(
                get_discount_cash_flow_value(
                    [1, 1.5, 1.2, 1.1]
                ),2
            ), 
            4.58
        )

    def test_dcf_valuation_when_nan_in_record(self):
        assert_equal(
            round(
                get_discount_cash_flow_value(
                    [
                        nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, 0., 0., 
                        -0.47, -0.47, -0.235, -7.43670886, 
                        -4.23944783
                    ]
                ), 
                2
            ),
            nan
        )


if __name__ == '__main__':
    unittest.main()