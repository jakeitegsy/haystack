
from utilities import (
    analysis_folder, edgar_folder, industry_folder,
    stockpup_folder, sectors_folder, processed_folder,
    benchmark, munge_data
)
# from haystack_munger import Munge
from stock import Edgar, StockPup, AssignIndustry, AssignSector
from sectors import Sectors
from industries import Industries
from industry import Industry
from sector import Sector
from os import listdir
from multiprocessing import Pool

import time

def munge_sectors():
    """Clean SPDRS data and write files to sectors_folder, 
    removing extra info
    """
    # benchmark(
    #     report="SECTORS_MUNGE",
    #     job=munge_data,
    #     from_folder=sectors_folder(),
    #     to_folder=processed_folder(sectors_folder()),
    #     munger=Munge,
    # )
    benchmark(
        report="ASSIGN_SECTORS",
        job=munge_data,
        from_folder=processed_folder(sectors_folder()),
        to_folder=processed_folder(analysis_folder(sectors_folder())),
        munger=AssignSector,
    )

def assign_industries():
    """Clean Industry Data and Write import information to file"""
    benchmark(
        report="ASSIGN_INDUSTRIES",
        job=munge_data,
        from_folder=industry_folder(),
        to_folder=processed_folder(analysis_folder(industry_folder())),
        munger=AssignIndustry,
    )

def assign_sectors():
    """Clean Sector Data and Write import information to file"""
    benchmark(
        report="ASSIGN_SECTOR",
        job=munge_data,
        from_foler=sectors_folder(),
        to_folder=processed_folder(analysis_folder(sectors_folder())),
        munger=AssignSector,
    )

def analyze_stockpup():
    # Analyze StockPup data and write files to Analysis Folder
    benchmark(
        report="ANALYZE_STOCKPUP_DATA",
        job=munge_data,
        from_folder=processed_folder(stockpup_folder()),
        to_folder=processed_folder(
            analysis_folder(stockpup_folder())
        ),
        munger=StockPup,
    )
    
def analyze_edgar():
    # Analyze Edgar data and write files to Analysis Folder
    benchmark(
        report="ANALYZE_EDGAR_DATA",
        job=munge_data,
        from_folder=processed_folder(edgar_folder()),
        to_folder=processed_folder(analysis_folder(edgar_folder())),
        munger=Edgar,
    )

def stockpup_sectors():
    # Analyze StockPup data with SPDR Sector Information
    benchmark(
        report="ANALYZE_STOCKPUP_SECTORS",
        job=Sectors,
        sectors_folder=processed_folder(analysis_folder(sectors_folder())), 
        stocks_folder=processed_folder(analysis_folder(stockpup_folder())), 
        to_folder=processed_folder(analysis_folder(sectors_folder(stockpup_folder()))),
    )

def stockpup_industries():
    #Analyze StockPup data with NASDAQ Industry Information
    benchmark(
        report="ANALYZE_STOCKPUP_INDUSTRIES",
        job=Industries,
        industry_folder=processed_folder(analysis_folder(industry_folder())),
        stocks_folder=processed_folder(analysis_folder(stockpup_folder())),
        to_folder=processed_folder(analysis_folder(industry_folder(stockpup_folder()))),
    )

def edgar_sectors():
    # Analyze Edgar data with SPDR Sector Information
    benchmark(
        report="ANALYZE_EDGAR_SECTORS",
        job=Sectors,
        sectors_folder=processed_folder(analysis_folder(sectors_folder())),
        stocks_folder=processed_folder(analysis_folder(edgar_folder())),
        to_folder=processed_folder(analysis_folder(sectors_folder(edgar_folder()))),
    )

def edgar_industries():
    # Analyze Edgar data with NASDAQ Industry Information
    benchmark(
        report="ANALYZE_EDGAR_INDUSTRIES",
        job=Industries,
        industry_folder=processed_folder(analysis_folder(industry_folder())),
        stocks_folder=processed_folder(analysis_folder(edgar_folder())),
        to_folder=processed_folder(analysis_folder(industry_folder(edgar_folder()))),
    )
    
def write_stockpup_sector_reports():
    # Write Financial Reports on every stock in StockPup data by SPDR Sector
    benchmark(
        report="WRITE_SCORE_TOTAL_REPORTS",
        job=munge_data,
        from_folder=processed_folder(analysis_folder(sectors_folder(stockpup_folder()))),
        to_folder=processed_folder(analysis_folder(sectors_folder(stockpup_folder("score_total_reports/")))),
        munger=Sector,
    )

def write_stockpup_industry_reports():
    # Write Financial Reports on every stock in StockPup data by NASDAQ industry
    benchmark(
        report="WRITE_SCORE_TOTAL_REPORTS",
        job=munge_data,
        from_folder=processed_folder(analysis_folder(industry_folder(stockpup_folder()))),
        to_folder=processed_folder(analysis_folder(industry_folder(stockpup_folder("score_total_reports/")))),
        munger=Industry,
    )
    
def write_edgar_sector_reports():
    # Write Financial Reports on every stock in Edgar data by SPDR Sector
    benchmark(
        report="WRITE_SCORE_TOTAL_REPORTS",
        job=munge_data,
        from_folder=(f"{processed_folder()}{analysis_folder()}"
                     f"{sectors_folder()}{edgar_folder()}"),
        to_folder=(f"{processed_folder()}{analysis_folder()}"
                   f"{sectors_folder()}{edgar_folder()}"
                   "score_total_reports/"),
        munger=Sector,
    )

def write_edgar_industry_reports():
    # Write Financial Reports on every stock in Edgar by NASDAQ Industry
    benchmark(
        report="WRITE_SCORE_TOTAL_REPORTS",
        job=munge_data,
        from_folder=processed_folder(analysis_folder(industry_folder(edgar_folder()))),
        to_folder=processed_folder(analysis_folder(industry_folder(edgar_folder("score_total_reports/")))),
        munger=Industry,
    )

def time_it(function, *args, **kwargs):
    start_time = time.time()
    function(*args, **kwargs)
    print(f"Total Execution Time: {time.time()-start_time}  seconds\n")

def process_all_files(stock=None, source=None):
    failed = []
    for filename in listdir(source):
        try:
            stock(filename=filename).to_csv(processed_folder(source))
        except Exception as error:
            print('[ERROR]::Could not process::', filename)
            failed.append(filename)
    print('[FAILED]:', failed)

def process_file(stock, filename=None, source=None):
    try:
        stock(filename=filename).to_csv(processed_folder(source))
    except Exception as error:
        print('[ERROR]::Could not process::', filename)

def process_stockpup(filename):
    process_file(StockPup, filename=filename, source=stockpup_folder())

def process_edgar(filename):
    process_file(Edgar, filename=filename, source=edgar_folder())
    
def parallel_process_stock():
    with Pool(16) as pool:
        pool.map(process_stockpup, listdir(stockpup_folder()), 2) 
        pool.map(process_edgar, listdir(edgar_folder())) 
    # StockPup parallel with 16 processes = 86seconds
    # StockPup sequential = 117 seconds
    # StockPup with cpu_count = 97 seconds
    # StockPup with imap - no file creation - learn to use imap
    # StockPup with 100 processes = 127 seconds

if __name__ == '__main__':
    # munge_sectors()
    # assign_industries()
    # assign_sectors()
    # analyze_stockpup()
    # analyze_edgar()
    # stockpup_sectors()
    # stockpup_industries()
    # edgar_sectors()
    # edgar_industries()
    # write_stockpup_sector_reports()
    # write_stockpup_industry_reports()
    # write_edgar_sector_reports()
    # write_edgar_industry_reports()
    # time_it(process_all_files, stock=StockPup, source=stockpup_folder())
    # time_it(process_all_files, stock=Edgar, source=edgar_folder())
    time_it(parallel_process_stock)
    """
    StockPup Failures:
    ['BHF_quarterly_financial_data.csv', 'ZTS_quarterly_financial_data.csv', 
    '.DS_Store', 
    'INFO_quarterly_financial_data.csv', 'HOLX_quarterly_financial_data.csv', 'HSIC_quarterly_financial_data.csv', 'FTV_quarterly_financial_data.csv', 'KHC_quarterly_financial_data.csv', 'CHD_quarterly_financial_data.csv', 'QRVO_quarterly_financial_data.csv', 'KORS_quarterly_financial_data.csv', 'JNY_quarterly_financial_data.csv', 'BDK_quarterly_financial_data.csv', 'MHK_quarterly_financial_data.csv', 
    'WIKI-PRICES-2.csv', 
    'CSRA_quarterly_financial_data.csv', 'LSTR_quarterly_financial_data.csv', 
    'Flight Plan (1).docx']
    
    Edgar Failures:
    """

# get to a complete working state then go back and fine tune 
# how can I get to a MVP of the final product
# what is the simplest version of the final product I can have now?