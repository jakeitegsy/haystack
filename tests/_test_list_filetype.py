import unittest

from haystack_utilities import (
    list_filetype, EDGAR_FOLDER, SECTORS_FOLDER, STOCKPUP_FOLDER
)


class TestListFileType(unittest.TestCase):

    def test_list_filetype_returns_list_of_files_with_extension_in_the_folder(self):
        file_list = list_filetype(in_folder='benchmarks', extension="txt")
        self.assertGreater(len(file_list), 0)

    def test_list_filetype_returns_a_list_of_csvs_by_default(self):
        self.assertEqual(
            len(list_filetype(in_folder=STOCKPUP_FOLDER)),
            len(list_filetype(in_folder=STOCKPUP_FOLDER, extension="csv"))
        )

    def test_list_filetype_returns_empty_list_for_extensions_not_found(self):
        test_list = list_filetype(in_folder=EDGAR_FOLDER, extension='doc')
        self.assertEqual(len(test_list), 0)

    def test_list_filetype_returns_empty_list_for_illegal_extensions(self):
        test_list = list_filetype(in_folder=EDGAR_FOLDER, extension="bad ext")
        self.assertEqual(len(test_list), 0)


if __name__ == '__main__':
    unittest.main()