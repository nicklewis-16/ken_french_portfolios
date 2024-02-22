import pandas as pd
from pandas.tseries.offsets import MonthEnd
import wrds
from pathlib import Path
import config

OUTPUT_DIR = Path(config.OUTPUT_DIR)
DATA_DIR = Path(config.DATA_DIR)
WRDS_USERNAME = config.WRDS_USERNAME

def pull_CRSP_stock_data(start_date, end_date, wrds_username=WRDS_USERNAME):
    """
    Pulls necessary CRSP monthly stock data for NYSE, AMEX, and NASDAQ stocks,
    including monthly returns, prices, shares outstanding, and dividends.
    """
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
    db = wrds.Connection(wrds_username=wrds_username)
    crsp_data = db.raw_sql(sql_query, date_cols=['date'])
    db.close()

    # Adjust date to end of month
    crsp_data['jdate'] = crsp_data['date'] + MonthEnd(0)

    return crsp_data

def pull_CRSP_Comp_Link_Table(wrds_username=WRDS_USERNAME):
    """
    Pulls the linkage table between CRSP and Compustat to facilitate merging datasets.
    """
    sql_query = """
        SELECT 
            gvkey, lpermno AS permno, linktype, linkprim, linkdt, linkenddt
        FROM 
            crsp.ccmxpf_linktable
        WHERE 
            substr(linktype,1,1)='L' AND 
            (linkprim ='C' OR linkprim='P')
    """
    db = wrds.Connection(wrds_username=wrds_username)
    link_table = db.raw_sql(sql_query, date_cols=["linkdt", "linkenddt"])
    db.close()

    return link_table

def pull_Compustat_data(start_date, end_date, wrds_username=WRDS_USERNAME):
    """
    Pulls Compustat data including earnings and cash flow to identify firms with non-negative earnings.
    """
    sql_query = f"""
        SELECT 
            gvkey, datadate, ni, oancf -- oancf for operating activities net cash flow
        FROM 
            comp.funda
        WHERE 
            datadate BETWEEN '{start_date}' AND '{end_date}' AND
            indfmt='INDL' AND 
            datafmt='STD' AND 
            popsrc='D' AND 
            consol='C'
    """
    db = wrds.Connection(wrds_username=wrds_username)
    comp_data = db.raw_sql(sql_query, date_cols=['datadate'])
    db.close()

    # Filter for non-negative earnings
    comp_data = comp_data[comp_data['ni'] > 0]

    return comp_data

def main():
    start_date = '1951-07-01'
    end_date = '2023-12-31'

    crsp_data = pull_CRSP_stock_data(start_date, end_date, WRDS_USERNAME)
    link_table = pull_CRSP_Comp_Link_Table(WRDS_USERNAME)
    comp_data = pull_Compustat_data(start_date, end_date, WRDS_USERNAME)

    merged_data = pd.merge(crsp_data, link_table, left_on='permno', right_on='permno', how='inner')
    merged_data = pd.merge(merged_data, comp_data, left_on='gvkey', right_on='gvkey', how='inner')

    merged_data.to_parquet(DATA_DIR / "pulled" / "merged_CRSP_Compustat_data.parquet")

if __name__ == "__main__":
    main()
