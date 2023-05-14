from src.logger import Logger
from numpy import inf, nan


class Munger:

    def __init__(self,
        ticker=None, raw_data=None, mappings=None, filename=None
    ):
        self.logger = Logger(ticker)
        self.munge_data(raw_data=raw_data, mappings=mappings, filename=filename)

    def munge_data(self, mappings=None, raw_data=None, filename=None):
        if len(raw_data) > 1:
            self.munged_data = self.rename_columns(
                dataframe=self.convert_2000_new_year_to_1999_year_end(
                    raw_data
                ),
                mappings=mappings
            )
            self.logger.success(f"Munging data from File::{filename}::")
        else:
            self.logger.error(f"Munging data from File::{filename}::")
            self.munged_data = None

    def convert_2000_new_year_to_1999_year_end(self, dataframe):
        self.logger.log('Converted 1999-12-31 to 2000-01-01')
        try:
            return dataframe.replace('2000-01-01', '1999-12-31')
        except KeyError:
            return dataframe

    def set_uppercase_column_names(self, dataframe):
        self.logger.log('Setting Column Names to UPPERCASE Labels')
        return dataframe.rename(str.upper, axis='columns')

    def rename_columns(self, dataframe=None, mappings=None):
        self.logger.log('Converting Column Names')
        return self.set_uppercase_column_names(dataframe).rename(
            columns=mappings
        )