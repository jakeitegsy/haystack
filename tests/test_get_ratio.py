import unittest
#import http

from haystack_analyst import Analyst
from haystack_utilities import os, pd, janitor, TEST_FOLDER
#nan = pd.np.nan
pdt = pd.testing


class TestGetRatio(unittest.TestCase):

    def make_series(self, elements):
        of_length = self.length
        return pd.Series(elements,
                         index=range(len(elements)),
                         dtype=pd.np.float64)

    def setUp(self):
        self.get_ratio = Analyst().get_ratio
        self.length = 4

        self.positive_series = self.make_series([4, 3, 2, 1])
        self.zero_series = self.make_series([0]*4)
        self.negative_series = self.make_series([-4, -3, -2, -1])
        self.negative_identity_series = self.make_series([-1]*self.length)
        self.positive_two_series = self.make_series([2]*self.length)
        self.negative_ratio_series = self.make_series([-2.0, -1.5, -1.0, -0.5])

        self.positive = 1
        self.zero = 0
        self.negative = -1

    def test_get_ratio_returns_negative_number_when_numerator_is_zero(self):
        self.assertEqual(self.get_ratio(self.zero, self.positive), self.negative)
        self.assertEqual(self.get_ratio(self.zero, self.zero), self.zero)
        self.assertEqual(self.get_ratio(self.zero, self.negative), self.negative)

    def test_get_ratio_returns_negative_ratio_when_denominator_is_less_than_zero(self):
        self.assertEqual(self.get_ratio(self.positive, -2), -0.5)
        self.assertEqual(self.get_ratio(self.zero, self.negative), self.negative)
        self.assertEqual(self.get_ratio(self.negative, -2), -0.5)

    def test_get_ratio_returns_negative_ratio_when_numerator_is_less_than_zero_and_y_is_greater_than_zero(self):
        self.assertEqual(
            self.get_ratio(self.negative, self.positive), self.negative
        )
        self.assertEqual(self.get_ratio(self.negative, 2), -0.5)

    def test_get_ratio_returns_negative_numerator_when_numerator_is_less_than_zero_and_y_is_zero(self):
        self.assertEqual(self.get_ratio(self.negative, self.zero), self.negative)

    def test_get_ratio_returns_numerator_when_denominator_is_zero(self):
        self.assertEqual(self.get_ratio(self.positive, self.zero), self.positive)
        self.assertEqual(self.get_ratio(2, self.zero), 2)

    def test_get_ratio_returns_ratio_when_both_numerator_and_denominator_are_positive(self):
        self.assertEqual(
            self.get_ratio(self.positive, self.positive), self.positive
        )
        self.assertEqual(self.get_ratio(1, 2), 0.5)
        self.assertEqual(self.get_ratio(2, 1), 2.0)

    def test_get_ratio_returns_negative_series_when_numerator_is_zero(self):
        pdt.assert_series_equal(
            self.get_ratio(self.zero_series, self.positive_series),
            self.negative_series
        )
        pdt.assert_series_equal(
            self.get_ratio(self.zero_series, self.zero_series),
            self.zero_series
        )
        pdt.assert_series_equal(
            self.get_ratio(self.zero_series, self.negative_series),
            self.negative_series
        )

    def test_get_ratio_returns_negative_ratio_series_when_denominator_is_less_than_zero(self):
        pdt.assert_series_equal(
            self.get_ratio(self.positive_series, self.negative_series),
            self.negative_identity_series
        )
        pdt.assert_series_equal(
            self.get_ratio(self.negative_series, self.negative_series),
            self.negative_identity_series
        )

    def test_get_ratio_returns_negative_ratio_series_when_numerator_is_less_than_zero_and_y_is_greater_than_zero(self):
        pdt.assert_series_equal(
            self.get_ratio(self.negative_series, self.positive_series), 
            self.negative_identity_series
        )
        pdt.assert_series_equal(
            self.get_ratio(
                self.negative_series, self.positive_two_series
            ), 
            self.negative_ratio_series
        )

    def test_get_ratio_returns_negative_series_when_numerator_is_less_than_zero_and_y_is_zero(self):
        pdt.assert_series_equal(self.get_ratio(
            self.negative_series, self.zero_series
            ),
            self.negative_series
        )

    def test_get_ratio_returns_numerator_series_when_denominator_is_zero(self):
        pdt.assert_series_equal(
            self.get_ratio(self.positive_series, self.zero_series),
            self.positive_series
        )

    def test_get_ratio_returns_ratio_series_when_both_numerator_and_denominator_are_positive(self):
        pdt.assert_series_equal(
            self.get_ratio(self.positive_series, self.positive_series),     
            -(self.negative_identity_series)
        )
        pdt.assert_series_equal(self.get_ratio(
                self.positive_series, self.positive_two_series
            ), -(self.negative_ratio_series)
        )


if __name__ == '__main__':
    unittest.main()