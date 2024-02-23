from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from pathlib import Path
import wrds
import numpy as np
import config

DATA_DIR = Path(config.DATA_DIR)
WRDS_USERNAME = config.WRDS_USERNAME

START_DATE = '1951-07-01'  
END_DATE = '2023-12-31'    

def establish_db_connection(wrds_username=WRDS_USERNAME):
    """
    Establishes a connection to the WRDS database.
    """
    return wrds.Connection(wrds_username=wrds_username)

def pull_CRSP_stock_data(start_date=START_DATE, end_date=END_DATE, wrds_username=WRDS_USERNAME):
    """
    Pulls necessary CRSP monthly stock data for NYSE, AMEX, and NASDAQ stocks.
    """
    start_date = (datetime.strptime(start_date, "%Y-%m-%d") - relativedelta(months=1)).strftime("%Y-%m-%d")

    sql_query = f"""
        SELECT 
            msf.permno, msf.permco, msf.date, 
            msf.ret AS mthret, msf.ret AS mthretx, msf.shrout, msf.prc AS mthprc
        FROM 
            crsp.msf AS msf
            JOIN crsp.msenames AS names ON msf.permno = names.permno 
            AND msf.date BETWEEN names.namedt AND names.nameendt
        WHERE 
            msf.date BETWEEN '{start_date}' AND '{end_date}' AND
            names.exchcd IN (1, 2, 3)
    """

    with establish_db_connection(wrds_username) as db:
        crsp_data = db.raw_sql(sql_query, date_cols=['date'])

    crsp_data['date'] = pd.to_datetime(crsp_data['date'])
    crsp_data['jdate'] = crsp_data['date'] + pd.offsets.MonthEnd(0)

    return crsp_data

def pull_Compustat_data(start_date=START_DATE, end_date=END_DATE, wrds_username=WRDS_USERNAME):
    """
    Pulls Compustat data including earnings, cash flow, and earnings per share (EPS).
    """
    sql_query = f"""
        SELECT 
            gvkey, datadate, ni, oancf, epsfx
        FROM 
            comp.funda
        WHERE 
            datadate BETWEEN '{start_date}' AND '{end_date}' AND
            indfmt='INDL' AND 
            datafmt='STD' AND 
            popsrc='D' AND 
            consol='C'
    """

    with establish_db_connection(wrds_username) as db:
        comp_data = db.raw_sql(sql_query, date_cols=['datadate'])

    comp_data = comp_data[comp_data['ni'] > 0]

    return comp_data

def pull_ccm_link_table(wrds_username=WRDS_USERNAME):
    """
    Pulls the CCM link table data from WRDS.
    """
    sql_query = """
        SELECT gvkey, lpermno as permno, linkdt, linkenddt
        FROM crsp.ccmxpf_linktable
        WHERE usedflag = 1
    """
    
    with establish_db_connection(wrds_username) as db:
        link_table = db.raw_sql(sql_query)
        
    link_table['linkdt'] = pd.to_datetime(link_table['linkdt'])
    link_table['linkenddt'] = pd.to_datetime(link_table['linkenddt'])
    
    # Adjust linkenddt for active links
    link_table['linkenddt'] = link_table['linkenddt'].fillna(pd.Timestamp('today'))
    
    return link_table

def match_crsp_compustat(df_crsp, df_compustat, df_link):
    """
    Matches CRSP and Compustat data using the CCM link table.
    """
    # Merge CRSP data with the link table based on permno and check date ranges
    df_merged = pd.merge(df_crsp, df_link, on='permno')
    df_merged = df_merged[(df_merged['date'] >= df_merged['linkdt']) & (df_merged['date'] <= df_merged['linkenddt'])]
    
    # Convert Compustat's datadate to the end of the month to match CRSP's jdate
    df_compustat['datadate_eom'] = df_compustat['datadate'] + pd.offsets.MonthEnd(0)
    
    # Merge the matched CRSP data with Compustat data based on gvkey and adjusted datadate
    df_final = pd.merge(df_merged, df_compustat, left_on=['gvkey', 'jdate'], right_on=['gvkey', 'datadate_eom'], how='left')
    
    return df_final


def load_CRSP_data(data_dir=DATA_DIR):
    filepath = Path(data_dir) / "pulled" / "CRSP_data.parquet"
    return pd.read_parquet(filepath)

def load_Compustat_data(data_dir=DATA_DIR):
    filepath = Path(data_dir) / "pulled" / "Compustat_data.parquet"
    return pd.read_parquet(filepath)

def demo():
    df_msf = load_CRSP_data(data_dir=DATA_DIR)
    df_msix = load_Compustat_data(data_dir=DATA_DIR)
    df_matched = match_crsp_compustat(data=DATA_DIR)

if __name__ == "__main__":
    
    df_msf = pull_CRSP_stock_data(START_DATE, END_DATE, WRDS_USERNAME)
    df_msix = pull_Compustat_data(START_DATE, END_DATE, WRDS_USERNAME)
    
    df_link = pull_ccm_link_table(WRDS_USERNAME)
    
    df_matched = match_crsp_compustat(df_msf, df_msix, df_link)

    df_msf.to_parquet(DATA_DIR / "pulled" / "CRSP_data.parquet")
    df_msix.to_parquet(DATA_DIR / "pulled" / "Compustat_data.parquet")
    df_matched.to_parquet(DATA_DIR / "pulled" / "merged_CRSP_Compustat_data.parquet")

