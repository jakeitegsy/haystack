import jadecobra.tester
import unittest

from src.utilities import (
    benchmark, list_filetype, os,
    testing_folder, stockpup_folder, sectors_folder
)


class TestBenchmark(jadecobra.tester.TestCase):

    def to_folder(self):
        return testing_folder('benchmarks/')

    def test_benchmark_writes_report_to_given_folder_default_name(self):
        test_job = list_filetype
        benchmark(
            job=test_job,
            folder=self.to_folder(),
            in_folder=stockpup_folder()
        )
        self.assertTrue(
            os.path.exists(
                f'{self.to_folder()}{test_job.__name__}_benchmark.txt'
            )
        )

    def test_benchmark_writes_report_to_given_folder_with_given_name(self):
        benchmark(
            report='bob',
            job=list_filetype,
            folder=self.to_folder(),
            in_folder=sectors_folder()
        )
        self.assertTrue(
            os.path.exists(f'{self.to_folder()}bob_benchmark.txt')
        )