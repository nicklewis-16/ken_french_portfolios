

import pandas as pd
from pandas.tseries.offsets import MonthEnd, YearEnd

import numpy as np
import wrds

import config
from pathlib import Path

OUTPUT_DIR = Path(config.OUTPUT_DIR)
DATA_DIR = Path(config.DATA_DIR)
WRDS_USERNAME = config.WRDS_USERNAME
WRDS_PASSWORD = config.WRDS_PASSWORD
START_DATE = config.START_DATE
END_DATE = config.END_DATE

import pandas as pd
import numpy as np
import sqlite3

from plotnine import *
from mizani.formatters import comma_format, percent_format
from datetime import datetime


start_date = "01/01/1960"
end_date = "02/28/2024"

from sqlalchemy import create_engine

import os
from dotenv import load_dotenv
load_dotenv()

connection_string = (
  "postgresql+psycopg2://"
  f"{WRDS_USERNAME}:{WRDS_PASSWORD}"
  "@wrds-pgdata.wharton.upenn.edu:9737/wrds"
)

wrds = create_engine(connection_string, pool_pre_ping=True)

def pull_CRSP_stock():
    """
    Pulls CRSP stock data from the WRDS database.

    Parameters:
    - wrds_username (str): WRDS username (default: WRDS_USERNAME)

    Returns:
    - crsp_monthly (pd.DataFrame): CRSP stock data

    """
    crsp_monthly_query = (
    "SELECT msf.permno, msf.permco, msf.date, "
            "date_trunc('month', msf.date)::date as month, "
            "msf.ret, msf.retx, msf.shrout, msf.altprc, "
            "msenames.exchcd, msenames.siccd, "
            "msedelist.dlret, msedelist.dlstcd "
        "FROM crsp.msf AS msf "
        "LEFT JOIN crsp.msenames as msenames "
        "ON msf.permno = msenames.permno AND "
        "msenames.namedt <= msf.date AND "
        "msf.date <= msenames.nameendt "
        "LEFT JOIN crsp.msedelist as msedelist "
        "ON msf.permno = msedelist.permno AND "
        "date_trunc('month', msf.date)::date = "
        "date_trunc('month', msedelist.dlstdt)::date "
        f"WHERE msf.date BETWEEN '{start_date}' AND '{end_date}' "
        "AND msenames.shrcd IN (10, 11)"
    )

    crsp_monthly = (pd.read_sql_query(
        sql=crsp_monthly_query,
        con=wrds,
        dtype={"permno": int, "exchcd": int, "siccd": int},
        parse_dates={"date", "month"})
    .assign(shrout=lambda x: x["shrout"]*1000)
    )
    crsp_monthly = (crsp_monthly
    .assign(me=lambda x: abs(x["shrout"]*x["altprc"]/1000000))
    .assign(me=lambda x: x["me"].replace(0, np.nan))
    )

    me_lag = (crsp_monthly
    .assign(
        month=lambda x: x["month"]+pd.DateOffset(months=1),
        me_lag=lambda x: x["me"]
    )
    .get(["permno", "month", "me_lag"])
    )

    crsp_monthly = (crsp_monthly
    .merge(me_lag, how="left", on=["permno", "month"])
    )

    def assign_exchange(exchcd):
        if exchcd in [1, 31]:
            return "NYSE"
        elif exchcd in [2, 32]:
            return "AMEX"
        elif exchcd in [3, 33]:
            return "NASDAQ"
        else:
            return "Other"

    crsp_monthly["exchange"] = (crsp_monthly["exchcd"]
        .apply(assign_exchange)
    )
    
    crsp_monthly = crsp_monthly[crsp_monthly['exchange'].isin(['NYSE', 'NASDAQ', 'AMEX'])]
    
    conditions_delisting = [
        crsp_monthly["dlstcd"].isna(), 
        (~crsp_monthly["dlstcd"].isna()) & (~crsp_monthly["dlret"].isna()),
        crsp_monthly["dlstcd"].isin([500, 520, 580, 584]) | 
            ((crsp_monthly["dlstcd"] >= 551) & (crsp_monthly["dlstcd"] <= 574)),
        crsp_monthly["dlstcd"] == 100
    ]

    choices_delisting = [
        crsp_monthly["ret"],
        crsp_monthly["dlret"],
        -0.30,
        crsp_monthly["ret"]
    ]

    crsp_monthly = (crsp_monthly
    .assign(
        ret_adj=np.select(conditions_delisting, choices_delisting, default=-1)
    )
    .drop(columns=["dlret", "dlstcd"])
    )

    return crsp_monthly
    

def pull_compustat():
    """
    Pulls financial data from the Compustat database for a specified time period.

    Parameters:
    - wrds_username (str): The username for accessing the WRDS database. Defaults to WRDS_USERNAME.

    Returns:
    - compustat (DataFrame): A DataFrame containing the financial data from the Compustat database.

    """
    compustat_query = (
    "SELECT gvkey, datadate, seq, ceq, at, lt, txditc, txdb, itcb,  pstkrv, "
            "pstkl, pstk, capx, oancf, sale, cogs, xint, xsga, sich, ni, ebit, dp "
        "FROM comp.funda "
        "WHERE indfmt = 'INDL' "
            "AND datafmt = 'STD' "
            "AND consol = 'C' "
            f"AND datadate BETWEEN '{start_date}' AND '{end_date}'"
    )

    compustat = pd.read_sql_query(
    sql=compustat_query,
    con=wrds,
    dtype={"gvkey": str},
    parse_dates={"datadate"}
    )
    compustat = (compustat
    .assign(
        be=lambda x: 
        (x["seq"].combine_first(x["ceq"]+x["pstk"])
        .combine_first(x["at"]-x["lt"])+
        x["txditc"].combine_first(x["txdb"]+x["itcb"]).fillna(0)-
        x["pstkrv"].combine_first(x["pstkl"])
        .combine_first(x["pstk"]).fillna(0))
    )
    .assign(
        be=lambda x: x["be"].apply(lambda y: np.nan if y <= 0 else y)
    )
    .assign(
        op=lambda x: 
        ((x["sale"]-x["cogs"].fillna(0)- 
            x["xsga"].fillna(0)-x["xint"].fillna(0))/x["be"])
    )
    )
    compustat = (compustat
    .assign(year=lambda x: pd.DatetimeIndex(x["datadate"]).year)
    .sort_values("datadate")
    .groupby(["gvkey", "year"])
    .tail(1)
    .reset_index()
    )
    compustat_lag = (compustat
    .get(["gvkey", "year", "at"])
    .assign(year=lambda x: x["year"]+1)
    .rename(columns={"at": "at_lag"})
    )

    compustat = (compustat
    .merge(compustat_lag, how="left", on=["gvkey", "year"])
    .assign(inv=lambda x: x["at"]/x["at_lag"]-1)
    .assign(inv=lambda x: np.where(x["at_lag"] <= 0, np.nan, x["inv"]))
    )
    comp = compustat
    # number of years in Compustat
    comp = comp.sort_values(by=["gvkey", "datadate"])
    comp["count"] = comp.groupby(["gvkey"]).cumcount()
    
    return comp


def pull_CRSP_Comp_Link_Table(crsp_monthly):
    """
    Pulls the CRSP Compustat Link Table from the WRDS database and merges it with the CRSP monthly data.

    Parameters:
    - wrds_username (str): The username for accessing the WRDS database.
    - crsp_monthly (pd.DataFrame): The CRSP monthly data.

    Returns:
    - pd.DataFrame: The merged data containing the CRSP Compustat Link Table.

    """

    ccmxpf_linktable_query = (
    "SELECT lpermno AS permno, gvkey, linkdt, "
            "COALESCE(linkenddt, CURRENT_DATE) AS linkenddt "
        "FROM crsp.ccmxpf_linktable "
        "WHERE linktype IN ('LU', 'LC') "
            "AND linkprim IN ('P', 'C') "
            "AND usedflag = 1"
    )

    ccmxpf_linktable = pd.read_sql_query(
    sql=ccmxpf_linktable_query,
    con=wrds,
    dtype={"permno": int, "gvkey": str},
    parse_dates={"linkdt", "linkenddt"}
    )
    ccm_links = (crsp_monthly
    .merge(ccmxpf_linktable, how="inner", on="permno")
    .query("~gvkey.isnull() & (date >= linkdt) & (date <= linkenddt)")
    .get(["permno", "gvkey", "date"])
    )

    crsp_monthly = (crsp_monthly
    .merge(ccm_links, how="left", on=["permno", "date"])
    )
    
    return crsp_monthly



def load_compustat(data_dir=DATA_DIR):
    path = Path(data_dir) / "pulled" / "Compustat.parquet"
    comp = pd.read_parquet(path)
    return comp


def load_CRSP_stock(data_dir=DATA_DIR):
    path = Path(data_dir) / "pulled" / "CRSP_stock.parquet"
    crsp = pd.read_parquet(path)
    return crsp


def load_CRSP_Comp_Link_Table(data_dir=DATA_DIR):
    path = Path(data_dir) / "pulled" / "CRSP_Comp_Link_Table.parquet"
    ccm = pd.read_parquet(path)
    return ccm


def _demo():
    comp = load_compustat()
    crsp = load_CRSP_stock()
    ccm = load_CRSP_Comp_Link_Table(crsp_monthly=crsp)


if __name__ == "__main__":
    comp = pull_compustat()
    comp.to_parquet(DATA_DIR / "pulled" / "Compustat.parquet")

    crsp = pull_CRSP_stock()
    crsp.to_parquet(DATA_DIR / "pulled" / "CRSP_stock.parquet")

    ccm = pull_CRSP_Comp_Link_Table(crsp_monthly=crsp)
    ccm.to_parquet(DATA_DIR / "pulled" / "CRSP_Comp_Link_Table.parquet")
