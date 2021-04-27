import unittest

from haystack_analyst import Analyst


class TestJudgeGrowth(unittest.TestCase):

    def setUp(self):
        self.judge = Analyst().judge_growth

    def test_judge_returns_GREW_if_growth_rate_of_x_to_y_is_greater_than_0(self):
        self.assertEqual(self.judge(0.0001), "grew")

    def test_judge_returns_SHRANK_if_growth_rate_of_x_to_y_is_less_than_0(self):
        self.assertEqual(self.judge(-0.0001), "shrank")

    def test_judge_returns_IDLED_if_growth_rate_of_x_to_y_is_equal_to_0(self):
        self.assertEqual(self.judge(0), "idled")

    def test_judge_returns_IGNORE_for_invalid_input(self):
        self.assertEqual(self.judge("a"), "ignore")    


if __name__ == '__main__':
    unittest.main()