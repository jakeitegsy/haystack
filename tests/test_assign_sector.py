import unittest

from stock import AssignSector
from pandas import DataFrame
from os import path
from utilities import (
    processed_folder, sectors_folder, testing_folder, analysis_folder, random_ticker
)


class TestSectors(unittest.TestCase):

    def setUp(self):
        self.ticker = random_ticker(
            from_folder=f"{processed_folder}{sectors_folder}"
        )
        self.sector = AssignSector(
            self.ticker,
            to_folder=testing_folder(processed_folder(analysis_folder(sectors_folder())))
        )
    def test_sector_contains_10_items(self):
        self.assertEqual(len(self.sector.sectors), 10)
    """
    def test_sector_XLY_returns_consumer_discretionary(self):
        self.assertEqual(
            self.sector.sectors['XLY'], 'CONSUMER_DISCRETIONARY'
        )

    def test_sector_XLP_returns_consumer_staples(self):
        self.assertEqual(self.sector.sectors['XLP'], 'CONSUMER_STAPLES')

    def test_sector_XLE_returns_energy(self):
        self.assertEqual(self.sector.sectors['XLE'], 'ENERGY')

    def test_sector_XLF_returns_financials(self):
        self.assertEqual(self.sector.sectors['XLF'], 'FINANCIALS')

    def test_sector_XLV_returns_healthcare(self):
        self.assertEqual(self.sector.sectors['XLV'], 'HEALTHCARE')

    def test_sector_XLI_returns_industrials(self):
        self.assertEqual(self.sector.sectors['XLI'], 'INDUSTRIALS')

    def test_sector_XLB_returns_materials(self):
        self.assertEqual(self.sector.sectors['XLB'], 'MATERIALS')

    def test_sector_XLRE_returns_real_estate(self):
        self.assertEqual(self.sector.sectors['XLRE'], 'REAL_ESTATE')

    def test_sector_XLK_returns_technology(self):
        self.assertEqual(self.sector.sectors['XLK'], 'TECHNOLOGY')

    def test_sector_XLU_returns_utilities(self):
        self.assertEqual(self.sector.sectors['XLU'], 'UTILITIES')
    """
    
@unittest.skip
class TestAssignSectors(TestSectors):
    
    def setUp(self):
        super().setUp()
        self.symbols = self.sector.symbols

    def test_read_sectors_and_symbols_returns_dataframe(self):
        self.assertIs(type(self.symbols), DataFrame)

    def test_column_names(self):
        self.assertEqual(
            sorted(self.symbols.columns),
            ['COMPANY', 'SECTOR']
        )

    def test_index_name_is_sector_symbol(self):
        self.assertEqual(self.symbols.index.name, 'Symbol')

    def test_data_has_unique_rows(self):
        self.assertFalse(self.symbols.index.duplicated().all())

@unittest.skip
class TestWriteSector(TestSectors):

    def setUp(self):
        super().setUp()

    def test_write_sectors_writes_csv_to_sectors_folder(self):
        self.assertTrue(path.exists(f"{self.sector.folder}/"
                                       f"{self.ticker}.CSV"))


if __name__ == '__main__':
    unittest.main()