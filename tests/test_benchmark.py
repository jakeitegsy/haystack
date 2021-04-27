import unittest

from haystack_utilities import (
    benchmark, list_filetype, os,
    TEST_FOLDER, STOCKPUP_FOLDER, SECTORS_FOLDER
)


class TestBenchmark(unittest.TestCase):

    def setUp(self):
        self.to_folder = f'{TEST_FOLDER}benchmarks/'

    def test_benchmark_writes_report_to_given_folder_default_name(self):
        test_job = list_filetype
        benchmark(job=test_job,
                  folder=self.to_folder,
                  in_folder=STOCKPUP_FOLDER)
        self.assertTrue(os.path.exists(
                f'{self.to_folder}{test_job.__name__}_benchmark.txt'
            )
        )

    def test_benchmark_writes_report_to_given_folder_with_given_name(self):
        test_job = list_filetype
        test_name = 'bob'
        benchmark(report=test_name, job=test_job, folder=self.to_folder, 
                  in_folder=SECTORS_FOLDER)
        self.assertTrue(os.path.exists(
            f'{self.to_folder}{test_name}_benchmark.txt')
        )


