import datetime
import os
import pandas as pd
import random
import shutil
import sys
import time
import traceback

ANALYSIS_FOLDER = 'analysis/'
EDGAR_FOLDER = 'edgar_data/'
INDUSTRY_FOLDER = 'industry_data/'
PROCESSED_FOLDER = 'processed/'
SECTORS_FOLDER = 'sectors_data/'
STOCKPUP_FOLDER = 'stockpup_data/'
TEST_FOLDER = 'test_run/'

def benchmark(report=None, job=None, folder='benchmarks/', *args, **kwargs):
    if report is None:
        report = job.__name__
    if job is not None:
        makedir(folder)
        start_time = time.clock()
        print(f'.... starting {report} ....')
        job(*args, **kwargs)
        with open(f'{folder}{report}_benchmark.txt', 'a') as out_file:
            out_file.write(
                f"RUN:{datetime.datetime.now()}," 
                f"DURATION: {time.clock()-start_time}\n"
            )
        print(f'{report} is done ')

def janitor(folder):
    if os.path.exists(folder):
        shutil.rmtree(folder)

def list_filetype(in_folder=None, extension='csv'):
    """return sorted list of files with the extension or empty list
    """
    if in_folder is not None:
        try:
            return sorted(
                [filename for filename in os.listdir(in_folder) 
                          if filename.lower().endswith(f'.{extension}')]
            )
        except FileNotFoundError:
            return []

def makedir(folder):
    try:
        os.makedirs(folder)
    except (OSError, TypeError):
        pass

def munge_data(from_folder=None, to_folder=None, using=None, *args, **kwargs):
    '''Writes file to to_folder after performing function from using on file from_folder
    '''
    directory = list_filetype(in_folder=from_folder)
    successful = 0
    failed = []

    if directory is not None:
        for filename in directory:
            in_file = f'{from_folder}{filename}'
            try:
                using(filename=in_file, to_folder=to_folder, *args, **kwargs)
                successful += 1
            except Exception as error_msg:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                write_error(filename, error_msg, exc_type, exc_value, 
                                                 exc_traceback)
                failed.append(filename)
            else:
                pass

    if successful > 0:
        print(f'... Successfully munged {successful} files to {to_folder}...')

    failed_count = len(failed)
    if failed_count > 0:
        print(f'{failed_count} reports failed')
        [print(f'.... I could not munge {name} to {to_folder} ....')
               for name in failed]

def random_file(from_folder=None):
    """returns a random file from from_folder"""
    if from_folder is not None:
        file_list = list_filetype(in_folder=from_folder)
        try:
            random_filename = file_list[random.randrange(len(file_list))]
        except TypeError:
            return
        return f'{from_folder}{random_filename}'

def random_ticker(from_folder=None):
    """returns a random stock symbol from from_folder if data is preprocessed
       does not work correctly for filenames like XYZ_quarterly_financial_data
       filename must be in the format XYZ.abc
    """
    if from_folder is not None:
        filename = random_file(from_folder=from_folder)
        try:
            ticker = os.path.split(filename)[1]
            ticker = os.path.splitext(ticker)[0]
        except TypeError:
            return
        return ticker

def test_folder(to_folder):
    if to_folder[-1] != '/':  
        to_folder = f'{to_folder}/'
    return to_folder

def test_prep(from_folder=None, to_folder=None, using=None, **kwargs):
    """returns a valid stock symbol from folder with usable data"""
    if from_folder is not None or to_folder is not None:
        ticker = random_ticker(from_folder=from_folder)
        print(f".................... Trying {ticker} "
              f"from {using.__name__} data .....................")

        if ticker is not None:
            stock_object = using(
                ticker=ticker,
                from_folder=from_folder, 
                to_folder=to_folder,
                **kwargs,
            )

            while stock_object.data is None:
                stock_object = test_prep(
                    from_folder=from_folder,
                    to_folder=to_folder,
                    using=using,
                    **kwargs,
                )

            print(f"..................... Currently Testing {ticker} "
                  f"from {using.__name__} data .....................")

            return stock_object
        else:
            print("The ticker does not exist, trying another ticker")
    else:
        print("One of the folders does not exist, try again")

def write_error(filename, error_msg, exc_type, exc_value, exc_traceback):
    
    """print error reports to screen"""
    print(f"{filename}'s reports could not be written because ")
    traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=2, file=sys.stdout)
