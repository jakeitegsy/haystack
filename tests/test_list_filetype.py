import unittest

from utilities import (
    list_filetype, edgar_folder, sectors_folder, stockpup_folder
)


class TestListFileType(unittest.TestCase):

    def test_list_filetype_returns_list_of_files_with_extension_in_the_folder(self):
        file_list = list_filetype(
            in_folder='benchmarks', extension="txt"
        )
        self.assertGreater(len(file_list), 0)

    def test_list_filetype_returns_a_list_of_csvs_by_default(self):
        self.assertGreater(
            len(
                list_filetype(
                    in_folder=stockpup_folder(), extension="csv"
                )
            ), 
            0
        )

    def test_list_filetype_returns_empty_list_for_extensions_not_found(self):
        self.assertEqual(
            len(
                list_filetype(
                    in_folder=edgar_folder(), extension='doc'
                )
            ), 
            0
        )

    def test_list_filetype_returns_empty_list_for_illegal_extensions(self):
        self.assertEqual(
            len(
                list_filetype(
                    in_folder=sectors_folder(), extension="bad ext"
                )
            ), 
            0
        )


if __name__ == '__main__':
    unittest.main()