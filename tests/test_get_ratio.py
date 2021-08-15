import unittest
import pandas
import numpy

from stock import get_ratio
from haystack_utilities import os, TEST_FOLDER

def create_series(elements):
    return pandas.Series(
        elements,
        index=range(len(elements)),
        dtype=numpy.float64
    )

class TestGetRatio(unittest.TestCase):

    def positive_series(self):
        return create_series([4, 3, 2, 1])

    def zero_series(self):
        return create_series([0]*4)

    def negative_series(self):
        return create_series([-4, -3, -2, -1])

    def negative_identity_series(self):
        return create_series([-1]*4)

    def positive_two_series(self):
        return create_series([2]*4)

    def negative_floats_series(self):
        return create_series([-2.0, -1.5, -1.0, -0.5])

    def test_get_ratio_returns_negative_number_when_numerator_is_zero(self):
        self.assertEqual(get_ratio(0, 1), -1)
        self.assertEqual(get_ratio(0, 0), 0)
        self.assertEqual(get_ratio(0, -1), -1)

    def test_get_ratio_returns_negative_ratio_when_denominator_is_less_than_zero(self):
        self.assertEqual(get_ratio(1, -2), -0.5)
        self.assertEqual(get_ratio(0, -1), -1)
        self.assertEqual(get_ratio(-1, -2), -0.5)

    def test_get_ratio_returns_negative_ratio_when_numerator_is_less_than_zero_and_y_is_greater_than_zero(self):
        self.assertEqual(get_ratio(-1, 1), -1)
        self.assertEqual(get_ratio(-1, 2), -0.5)

    def test_get_ratio_returns_negative_numerator_when_numerator_is_less_than_zero_and_y_is_zero(self):
        self.assertEqual(get_ratio(-1, 0), -1)

    def test_get_ratio_returns_numerator_when_denominator_is_zero(self):
        self.assertEqual(get_ratio(1, 0), 1)
        self.assertEqual(get_ratio(2, 0), 2)

    def test_get_ratio_returns_ratio_when_both_numerator_and_denominator_are_positive(self):
        self.assertEqual(get_ratio(1, 1), 1)
        self.assertEqual(get_ratio(1, 2), 0.5)
        self.assertEqual(get_ratio(2, 1), 2.0)

    def test_get_ratio_returns_negative_series_when_numerator_is_zero(self):
        pandas.testing.assert_series_equal(
            get_ratio(self.zero_series(), self.positive_series()),
            self.negative_series()
        )
        pandas.testing.assert_series_equal(
            get_ratio(self.zero_series(), self.zero_series()),
            self.zero_series()
        )
        pandas.testing.assert_series_equal(
            get_ratio(self.zero_series(), self.negative_series()),
            self.negative_series()
        )

    def test_get_ratio_returns_negative_ratio_series_when_denominator_is_less_than_zero(self):
        pandas.testing.assert_series_equal(
            get_ratio(self.positive_series(), self.negative_series()),
            self.negative_identity_series()
        )
        pandas.testing.assert_series_equal(
            get_ratio(self.negative_series(), self.negative_series()),
            self.negative_identity_series()
        )

    def test_get_ratio_returns_negative_ratio_series_when_numerator_is_less_than_zero_and_y_is_greater_than_zero(self):
        pandas.testing.assert_series_equal(
            get_ratio(self.negative_series(), self.positive_series()), 
            self.negative_identity_series()
        )
        pandas.testing.assert_series_equal(
            get_ratio(self.negative_series(), self.positive_two_series()), 
            self.negative_floats_series()
        )

    def test_get_ratio_returns_negative_series_when_numerator_is_less_than_zero_and_y_is_zero(self):
        pandas.testing.assert_series_equal(
            get_ratio(self.negative_series(), self.zero_series()),
            self.negative_series()
        )

    def test_get_ratio_returns_numerator_series_when_denominator_is_zero(self):
        pandas.testing.assert_series_equal(
            get_ratio(self.positive_series(), self.zero_series()),
            self.positive_series()
        )

    def test_get_ratio_returns_ratio_series_when_both_numerator_and_denominator_are_positive(self):
        pandas.testing.assert_series_equal(
            get_ratio(self.positive_series(), self.positive_series()),     
            -(self.negative_identity_series())
        )
        pandas.testing.assert_series_equal(
            get_ratio(self.positive_series(), self.positive_two_series()), 
            -(self.negative_floats_series())
        )


if __name__ == '__main__':
    unittest.main()