import unittest

from haystack_utilities import makedir, os, janitor, TEST_FOLDER


class TestMakeDirAndJanitor(unittest.TestCase):

    def setUp(self):
        self.folder = f'{TEST_FOLDER}utilities'

    def test_make_dir_creates_folder_janitor_removes_folder(self):
        self.assertFalse(os.path.exists(self.folder))
        makedir(self.folder)
        self.assertTrue(os.path.exists(self.folder))
        janitor(self.folder)
        self.assertFalse(os.path.exists(self.folder))


if __name__ == '__main__':
    unittest.main()