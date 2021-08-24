import unittest

from os import path
from utilities import makedir, janitor, testing_folder


class TestMakeDirAndJanitor(unittest.TestCase):

    def test_folder(self):
        return testing_folder('utilities')

    def test_make_dir_creates_folder_janitor_removes_folder(self):
        self.assertFalse(path.exists(self.test_folder()))
        makedir(self.test_folder())
        self.assertTrue(path.exists(self.test_folder()))
        janitor(self.test_folder())
        self.assertFalse(path.exists(self.test_folder()))


if __name__ == '__main__':
    unittest.main()