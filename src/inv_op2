import pandas as pd
import numpy as np
from pathlib import Path
import config


OUTPUT_DIR = Path(config.OUTPUT_DIR)
DATA_DIR = Path(config.DATA_DIR)

# from load_CRSP_Compustat import *
# from load_CRSP_stock import *
from load_CRSP_Compustat import *
from load_CRSP_stock import *


# Blue print
# Load and merge CRSP and Compustat data- might need to check out load_CRSP_Compustat.py
# Calculate operating profitability and investment metrics- condensed into 1 function
# Assign firms to quintiles based on these metrics

# For each month:
#   For each of the 25 portfolios:
#       Calculate value-weighted returns
#       Store these returns for analysis

# Analysis can then proceed with these portfolio returns

def calculate_market_equity(crsp):
    crsp = crsp.sort_values(by=["date", "permco", "me"])

    ### Aggregate Market Cap ###
    # sum of me across different permno belonging to same permco a given date
    crsp_summe = crsp.groupby(["date", "permco"])["me"].sum().reset_index()

    # largest mktcap within a permco/date
    crsp_maxme = crsp.groupby(["date", "permco"])["me"].max().reset_index()

    # join by jdate/maxme to find the permno
    crsp1 = pd.merge(crsp, crsp_maxme, how="inner", on=["date", "permco", "me"])

    # drop me column and replace with the sum me
    crsp1 = crsp1.drop(["me"], axis=1)

    # join with sum of me to get the correct market cap info
    crsp2 = pd.merge(crsp1, crsp_summe, how="inner", on=["date", "permco"])

    # sort by permno and date and also drop duplicates
    crsp2 = crsp2.sort_values(by=["permno", "date"]).drop_duplicates()
    return crsp2


def use_dec_market_equity(crsp2):
    """
    Finally, ME at June and December
    were flagged since (1) December ME will be used to create Book-to-Market
    ratio (BEME) and (2) June ME has to be positive in order to be part of
    the portfolio.'

    """
    # keep December market cap
    crsp2["year"] = crsp2["date"].dt.year
    crsp2["month"] = crsp2["date"].dt.month
    decme = crsp2[crsp2["month"] == 12]
    decme = decme[["permno", "date", "me", "year"]].rename(
        columns={"me": "dec_me"}
    )

    ### July to June dates
    crsp2["ffdate"] = crsp2["date"] + MonthEnd(-6)
    crsp2["ffyear"] = crsp2["ffdate"].dt.year
    crsp2["ffmonth"] = crsp2["ffdate"].dt.month
    crsp2["1+retx"] = 1 + crsp2["retx"]
    crsp2 = crsp2.sort_values(by=["permno", "month"])

    # cumret by stock
    crsp2["cumretx"] = crsp2.groupby(["permno", "ffyear"])["1+retx"].cumprod()

    # lag cumret
    crsp2["L_cumretx"] = crsp2.groupby(["permno"])["cumretx"].shift(1)

    # lag market cap
    crsp2["L_me"] = crsp2.groupby(["permno"])["me"].shift(1)

    # if first permno then use me/(1+retx) to replace the missing value
    crsp2["count"] = crsp2.groupby(["permno"]).cumcount()
    crsp2["L_me"] = np.where(
        crsp2["count"] == 0, crsp2["me"] / crsp2["1+retx"], crsp2["L_me"]
    )

    # baseline me
    mebase = crsp2[crsp2["ffmonth"] == 1][["permno", "ffyear", "L_me"]].rename(
        columns={"L_me": "mebase"}
    )

    # merge result back together
    crsp3 = pd.merge(crsp2, mebase, how="left", on=["permno", "ffyear"])
    crsp3["wt"] = np.where(
        crsp3["ffmonth"] == 1, crsp3["L_me"], crsp3["mebase"] * crsp3["L_cumretx"]
    )

    decme["year"] = decme["year"] + 1
    decme = decme[["permno", "year", "dec_me"]]

    # Info as of June
    crsp3_jun = crsp3[crsp3["month"] == 6]

    crsp_jun = pd.merge(crsp3_jun, decme, how="inner", on=["permno", "year"])

    crsp_jun = crsp_jun.sort_values(by=["permno", "month"]).drop_duplicates()
    return crsp3, crsp_jun

def merge_CRSP_and_Compustat(crsp_jun, comp, ccm, crsp3):
    comp['month_num'] = comp['datadate'].dt.month
    comp['year'] = comp['datadate'].dt.year
    ccm['month_num'] = ccm['date'].dt.month
    ccm['year'] = ccm['date'].dt.year

    ccm1 = pd.merge(
        comp, ccm, how="inner", on=["gvkey", "year", "month_num"]
    )
    ccm1["yearend"] = ccm1["datadate"] + YearEnd(0)
    ccm1["date"] = ccm1["yearend"] + MonthEnd(6)
   
    ccm2 = ccm1[["gvkey", "permno", "datadate", "yearend", "date", "retx", "be", "op", "inv", "count", "year"]]

    op_df = ccm2.groupby(['year', 'permno'])['op'].sum().reset_index().rename(columns={'op': 'year_op'})
    ccm2 = pd.merge(ccm2, op_df, on=['year', 'permno'], how='left')

    inv_df = ccm2.groupby(['year', 'permno'])['inv'].sum().reset_index().rename(columns={'inv': 'year_inv'})
    ccm2 = pd.merge(ccm2, inv_df, on=['year', 'permno'], how='left')

    ccm2 = pd.merge(ccm2, crsp3[['permno', 'date', 'wt']], how="left", on=["permno", "date"])

    # link comp and crsp
    ccm_jun = pd.merge(crsp_jun, ccm2, how="inner", on=["permno", "date"])
    ccm_jun["beme"] = ccm_jun["be"] * 1000 / ccm_jun["dec_me"]

    return ccm2, ccm_jun

def assign_portfolio(data, sorting_variable, n_portfolios):
    """Assign portfolio for a given sorting variable."""
    
    breakpoints = (data
      .get(sorting_variable)
      .quantile(np.linspace(0, 1, num=n_portfolios+1), 
                interpolation="linear")
      .drop_duplicates()
    )
    breakpoints.iloc[0] = -np.Inf
    breakpoints.iloc[breakpoints.size-1] = np.Inf
    
    assigned_portfolios = pd.cut(
      data[sorting_variable],
      bins=breakpoints,
      labels=range(1, breakpoints.size),
      include_lowest=True,
      right=False
    )
    
    return assigned_portfolios

def name_ports(ccm2):
    ccm2['op_num'] = assign_portfolio(ccm2, 'year_op', 5)
    ccm2['inv_num'] = assign_portfolio(ccm2, 'year_inv', 5)

    ccm2['opport'] = ccm2['op_num'].replace({1: 'OP1', 2: 'OP2', 3: 'OP3', 4: 'OP4', 5: 'OP5'})
    ccm2['invport'] = ccm2['inv_num'].replace({1: 'INV1', 2: 'INV2', 3: 'INV3', 4: 'INV4', 5: 'INV5'})

    return ccm2


def wavg(group, avg_name, weight_name):
    """function to calculate value weighted return
    """
    d = group[avg_name]
    w = group[weight_name]
    try:
        return (d * w).sum() / w.sum()
    except ZeroDivisionError:
        return np.nan

def create_op_inv_portfolios(ccm4):
    """Create value-weighted Fama-French portfolios
    and provide count of firms in each portfolio.
    """
    # THIS CODE IS COMPLETED FOR YOU
    # monthly value-weigthed return
    vwret_m = (
        ccm4.groupby(["date", "opport", "invport"])
        .apply(wavg, "retx", "wt")
        .to_frame()
        .reset_index()
        .rename(columns={0: "vwret_m"})
    )
    #vwret_m["OIport"] = vwret_m["opport"] + vwret_m["invport"]

    # monthly equal-weigthed return
    eq_wavg = (lambda x: np.mean(x['retx']))

    ewret_m = (
        ccm4.groupby(["date", "opport", "invport"])
        .apply(eq_wavg)
        .to_frame()
        .reset_index()
        .rename(columns={0: "ewret_m"})
    )
    #ewret_m["OIport"] = ewret_m["opport"] + ewret_m["invport"]

    # yearly value-weigthed return
    vwret_y = (
        ccm4.groupby(["year", "opport", "invport"])
        .apply(wavg, "retx", "wt")
        .to_frame()
        .reset_index()
        .rename(columns={0: "vwret_y"})
    )
    #vwret_y["OIport"] = vwret_y["opport"] + vwret_y["invport"]

    # yearly equal-weigthed return
    #eq_wavg = (lambda x: np.mean(x['mthret']))

    ewret_y = (
        ccm4.groupby(["year", "opport", "invport"])
        .apply(eq_wavg)
        .to_frame()
        .reset_index()
        .rename(columns={0: "ewret_y"})
    )
    #ewret_y["OIport"] = ewret_y["opport"] + ewret_y["invport"]

    # firm count
    num_firms = (
        ccm4.groupby(["date", "opport", "invport"])["retx"]
        .count()
        .reset_index()
        .rename(columns={"retx": "n_firms"})
    )
    #num_firms["OIport"] = num_firms["opport"] + num_firms["invport"]
    #num_firms = num_firms.pivot(index="date", columns="OIport", values="n_firms")

    ## market cap
    # avg_market_cap = (lambda x: np.mean(x['mktcap']))

    # cap = (
    #     ccm4.groupby(["date", "opport", "invport"])
    #     .apply(avg_market_cap)
    #     .to_frame()
    #     .reset_index()
    #     .rename(columns={0: "m_cap"})
    # )
    # #cap["OIport"] = cap["opport"] + cap["invport"]

    return vwret_m, ewret_m, vwret_y, ewret_y, num_firms



if __name__ == "__main__":        
    ###########################
    ## Load Data
    ###########################

    comp = load_compustat(data_dir=DATA_DIR)
    crsp = load_CRSP_stock(data_dir=DATA_DIR)
    ccm = load_CRSP_Comp_Link_Table(data_dir=DATA_DIR)

    crsp2 = calculate_market_equity(ccm)
    crsp3, crsp_jun = use_dec_market_equity(crsp2)
    ccm2, ccm_jun = merge_CRSP_and_Compustat(crsp_jun, comp, ccm, crsp3)
    
    ############################
    ## Form OP INV Factors
    ############################
    ccm3 = name_ports(ccm2)
    vwret_m, ewret_m, vwret_y, eqret_y, num_firms = create_op_inv_portfolios(ccm3) # create op_inv_portfolios
