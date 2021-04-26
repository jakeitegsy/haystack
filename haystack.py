from haystack_utilities import (
    ANALYSIS_FOLDER, EDGAR_FOLDER, INDUSTRY_FOLDER,
    STOCKPUP_FOLDER, SECTORS_FOLDER, PROCESSED_FOLDER,
    benchmark, munge_data, list_filetype
)
from haystack_munger import Munge
from haystack_stock import Edgar, Stockpup
from haystack_analyst import AssignIndustry, AssignSector
from haystack_sectors import Sectors
from haystack_industries import Industries
from haystack_industry import Industry
from haystack_sector import Sector

def munge_sectors():
    """Clean SPDRS data and write files to SECTORS_FOLDER, removing
    extra info
    """
    benchmark(
        report="SECTORS_MUNGE",
        job=munge_data,
        from_folder=SECTORS_FOLDER,
        to_folder=f"{PROCESSED_FOLDER}{SECTORS_FOLDER}",
        using=Munge,
    )
    benchmark(
        report="ASSIGN_SECTORS",
        job=munge_data,
        from_folder=f"{PROCESSED_FOLDER}{SECTORS_FOLDER}",
        to_folder=(f"{PROCESSED_FOLDER}{ANALYSIS_FOLDER}"
                   f"{SECTORS_FOLDER}"),
        using=AssignSector,
    )

def assign_industries():
    """Clean Industry Data and Write import information to file"""
    benchmark(
        report="ASSIGN_INDUSTRIES",
        job=munge_data,
        from_folder=INDUSTRY_FOLDER,
        to_folder=(f"{PROCESSED_FOLDER}{ANALYSIS_FOLDER}"
                   f"{INDUSTRY_FOLDER}"),
        using=AssignIndustry,
    )

def assign_sectors():
    """Clean Sector Data and Write import information to file"""
    benchmark(
        report="ASSIGN_SECTOR",
        job=munge_data,
        from_foler=SECTORS_FOLDER,
        to_folder=(f"{PROCESSED_FOLDER}{ANALYSIS_FOLDER}"
                   f"{SECTORS_FOLDER}"),
        using=AssignSector,
    )  

def munge_stockpup():
    # Clean Stockpup data and write files to STOCKPUP_FOLDER
    benchmark(
        report="STOCKPUP_MUNGE",
        job=munge_data,
        from_folder=STOCKPUP_FOLDER,
        to_folder=f"{PROCESSED_FOLDER}{STOCKPUP_FOLDER}",
        using=Munge,
    )

def munge_edgar():
    # Clean Edgar data and write files to STOCKPUP_FOLDER
    benchmark(
        report="EDGAR_MUNGE",
        job=munge_data,
        from_folder=EDGAR_FOLDER,
        to_folder=f"{PROCESSED_FOLDER}{EDGAR_FOLDER}",
        using=Munge,
    )

def analyze_stockpup():
    # Analyze StockPup data and write files to Analysis Folder
    benchmark(
        report="ANALYZE_STOCKPUP_DATA",
        job=munge_data,
        from_folder=f"{PROCESSED_FOLDER}{STOCKPUP_FOLDER}",
        to_folder=(f"{PROCESSED_FOLDER}{ANALYSIS_FOLDER}"
                   f"{STOCKPUP_FOLDER}"),
        using=Stockpup,
    )
    
def analyze_edgar():
    # Analyze Edgar data and write files to Analysis Folder
    benchmark(
        report="ANALYZE_EDGAR_DATA",
        job=munge_data,
        from_folder=f"{PROCESSED_FOLDER}{EDGAR_FOLDER}",
        to_folder=f"{PROCESSED_FOLDER}{ANALYSIS_FOLDER}{EDGAR_FOLDER}",
        using=Edgar,
    )

def stockpup_sectors():
    # Analyze Stockpup data with SPDR Sector Information
    benchmark(
        report="ANALYZE_STOCKPUP_SECTORS",
        job=Sectors,
        sectors_folder=(f"{PROCESSED_FOLDER}{ANALYSIS_FOLDER}"
                        f"{SECTORS_FOLDER}"), 
        stocks_folder=(f"{PROCESSED_FOLDER}{ANALYSIS_FOLDER}"
                       f"{STOCKPUP_FOLDER}"), 
        to_folder=(f"{PROCESSED_FOLDER}{ANALYSIS_FOLDER}"
                   f"{SECTORS_FOLDER}{STOCKPUP_FOLDER}"),
    )

def stockpup_industries():
    #Analyze Stockpup data with NASDAQ Industry Information
    benchmark(
        report="ANALYZE_STOCKPUP_INDUSTRIES",
        job=Industries,
        industry_folder=(f"{PROCESSED_FOLDER}{ANALYSIS_FOLDER}"
                         f"{INDUSTRY_FOLDER}"),
        stocks_folder=(f"{PROCESSED_FOLDER}{ANALYSIS_FOLDER}"
                       f"{STOCKPUP_FOLDER}"),
        to_folder=(f"{PROCESSED_FOLDER}{ANALYSIS_FOLDER}"
                   f"{INDUSTRY_FOLDER}{STOCKPUP_FOLDER}")
    )

def edgar_sectors():
    # Analyze Edgar data with SPDR Sector Information
    benchmark(
        report="ANALYZE_EDGAR_SECTORS",
        job=Sectors,
        sectors_folder=(f"{PROCESSED_FOLDER}{ANALYSIS_FOLDER}"
                        f"{SECTORS_FOLDER}"),
        stocks_folder=(f"{PROCESSED_FOLDER}{ANALYSIS_FOLDER}"
                       f"{EDGAR_FOLDER}"),
        to_folder=(f"{PROCESSED_FOLDER}{ANALYSIS_FOLDER}"
                   f"{SECTORS_FOLDER}{EDGAR_FOLDER}"),
    )

def edgar_industries():
    # Analyze Edgar data with NASDAQ Industry Information
    benchmark(
        report="ANALYZE_EDGAR_INDUSTRIES",
        job=Industries,
        industry_folder=(f"{PROCESSED_FOLDER}{ANALYSIS_FOLDER}"
                        f"{INDUSTRY_FOLDER}"),
        stocks_folder=(f"{PROCESSED_FOLDER}{ANALYSIS_FOLDER}"
                       f"{EDGAR_FOLDER}"),
        to_folder=(f"{PROCESSED_FOLDER}{ANALYSIS_FOLDER}"
                   f"{INDUSTRY_FOLDER}{EDGAR_FOLDER}"),
    )
    
def write_stockpup_sector_reports():
    # Write Financial Reports on every stock in StockPup data by SPDR Sector
    benchmark(
        report="WRITE_SCORE_TOTAL_REPORTS",
        job=munge_data,
        from_folder=(f"{PROCESSED_FOLDER}{ANALYSIS_FOLDER}"
                     f"{SECTORS_FOLDER}{STOCKPUP_FOLDER}"),
        to_folder=(f"{PROCESSED_FOLDER}{ANALYSIS_FOLDER}"
                   f"{SECTORS_FOLDER}{STOCKPUP_FOLDER}"
                   "score_total_reports/"),
        using=Sector,
    )

def write_stockpup_industry_reports():
    # Write Financial Reports on every stock in StockPup data by NASDAQ industry
    benchmark(
        report="WRITE_SCORE_TOTAL_REPORTS",
        job=munge_data,
        from_folder=(f"{PROCESSED_FOLDER}{ANALYSIS_FOLDER}"
                     f"{INDUSTRY_FOLDER}{STOCKPUP_FOLDER}"),
        to_folder=(f"{PROCESSED_FOLDER}{ANALYSIS_FOLDER}"
                   f"{INDUSTRY_FOLDER}{STOCKPUP_FOLDER}"
                   "score_total_reports/"),
        using=Industry,
    )
    
def write_edgar_sector_reports():
    # Write Financial Reports on every stock in Edgar data by SPDR Sector
    benchmark(
        report="WRITE_SCORE_TOTAL_REPORTS",
        job=munge_data,
        from_folder=(f"{PROCESSED_FOLDER}{ANALYSIS_FOLDER}"
                     f"{SECTORS_FOLDER}{EDGAR_FOLDER}"),
        to_folder=(f"{PROCESSED_FOLDER}{ANALYSIS_FOLDER}"
                   f"{SECTORS_FOLDER}{EDGAR_FOLDER}"
                   "score_total_reports/"),
        using=Sector,
    )

def write_edgar_industry_reports():
    # Write Financial Reports on every stock in Edgar by NASDAQ Industry
    benchmark(
        report="WRITE_SCORE_TOTAL_REPORTS",
        job=munge_data,
        from_folder=(f"{PROCESSED_FOLDER}{ANALYSIS_FOLDER}"
                     f"{INDUSTRY_FOLDER}{EDGAR_FOLDER}"),
        to_folder=(f"{PROCESSED_FOLDER}{ANALYSIS_FOLDER}"
                   f"{INDUSTRY_FOLDER}{EDGAR_FOLDER}"
                   "score_total_reports/"),
        using=Industry,
    )

if __name__ == '__main__':
    munge_sectors()
    munge_stockpup()
    munge_edgar()
    assign_industries()
    assign_sectors()
    analyze_stockpup()
    analyze_edgar()
    stockpup_sectors()
    stockpup_industries()
    edgar_sectors()
    edgar_industries()
    write_stockpup_sector_reports()
    write_stockpup_industry_reports()
    write_edgar_sector_reports()
    write_edgar_industry_reports()