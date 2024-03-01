import pandas as pd
import numpy as np
from pathlib import Path
import config


OUTPUT_DIR = Path(config.OUTPUT_DIR)
DATA_DIR = Path(config.DATA_DIR)

from load_CRSP_Compustat import *
from load_CRSP_stock import *


comp = load_compustat(data_dir=DATA_DIR)
crsp = load_CRSP_stock_ciz(data_dir=DATA_DIR)
ccm = load_CRSP_Comp_Link_Table(data_dir=DATA_DIR)

def calculate_market_equity(crsp):
    """
    'There were cases when the same firm (permco) had two or more securities
    (permno) on the same date. For the purpose of ME for the firm, we
    aggregated all ME for a given permco, date. This aggregated ME was assigned
    to the CRSP permno that has the largest ME.
    """
    crsp['me'] = crsp['mthprc'] * crsp['shrout']
    agg_me = crsp.groupby(['jdate', 'permco'])['me'].sum().reset_index()
    max_me_idx = crsp.groupby(['jdate', 'permco'])['me'].max().reset_index()
    crsp = pd.merge(crsp, max_me_idx, how='inner', on=['jdate', 'permco', 'me'])
    crsp = crsp.drop('me', axis=1)
    crsp = crsp.merge(agg_me, how='inner', on=['jdate', 'permco'])
    crsp = crsp.sort_values(by=['permno', 'jdate']).drop_duplicates()
    
    return crsp


def use_dec_market_equity(crsp2):
    """
    Finally, ME at June and December
    were flagged since (1) December ME will be used to create Book-to-Market
    ratio (BEME) and (2) June ME has to be positive in order to be part of
    the portfolio.'

    """
    crsp2["year"] = crsp2["jdate"].dt.year
    crsp2["month"] = crsp2["jdate"].dt.month
    decme = crsp2[crsp2["month"] == 12]
    decme = decme[["permno", "mthcaldt", "jdate", "me", "year"]].rename(
        columns={"me": "dec_me"}
    )

    crsp2["ffdate"] = crsp2["jdate"] + MonthEnd(-6)
    crsp2["ffyear"] = crsp2["ffdate"].dt.year
    crsp2["ffmonth"] = crsp2["ffdate"].dt.month
    crsp2["1+retx"] = 1 + crsp2["mthretx"]
    crsp2 = crsp2.sort_values(by=["permno", "mthcaldt"])

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
    crsp_jun = crsp_jun[
        [
            "permno",
            "mthcaldt",
            "jdate",
            "sharetype",
            "securitytype",
            "securitysubtype",
            "usincflg",
            "issuertype",
            "primaryexch",
            "conditionaltype",
            "tradingstatusflg",
            "mthret",
            "me",
            "wt",
            "cumretx",
            "mebase",
            "L_me",
            "dec_me",
        ]
    ]
    crsp_jun = crsp_jun.sort_values(by=["permno", "jdate"]).drop_duplicates()
    return crsp3, crsp_jun


def subset_CRSP_to_common_stock_and_exchanges(crsp):
    """Subset to common stock universe and
    stocks traded on NYSE, AMEX and NASDAQ.

    NOTE:
        With the new CIZ format, it is not necessary to apply delisting
        returns, as they are already applied.
    """ 
    crsp_filtered = crsp[
        (crsp['sharetype'] == 'NS') &  
        (crsp['securitytype'] == 'EQTY') &  
        (crsp['securitysubtype'] == 'COM') &  
        (crsp['usincflg'] == 'Y') &  
        (crsp['issuertype'].isin(['ACOR', 'CORP'])) &  
        (crsp['primaryexch'].isin(['N', 'A', 'Q'])) &  
        (crsp['tradingstatusflg'] == 'A') &  
        (crsp['conditionaltype'] == 'RW')  
    ]

    return crsp_filtered

def sort_into_portfolios(data):
    """
    Sorts firms into portfolios based on E/P ratio, considering custom breakpoints
    using December market equity (dec_me).
    """
    # Fill missing values
    data['ni'].fillna(0, inplace=True)
    data['dec_me'].fillna(0, inplace=True)
    
    # Calculate E/P ratio using December ME
    data['e_p_ratio'] = np.where(data['dec_me'] > 0, data['ni'] / data['dec_me'], np.nan)
    
    data['portfolio'] = np.where(data['ni'] <= 0, 'Earnings <= 0', 'Other')
    
    # Calculate Quantiles and Deciles
    data['quantile'] = pd.qcut(data['e_p_ratio'], 5, labels=['Qnt 1', 'Qnt 2', 'Qnt 3', 'Qnt 4', 'Qnt 5'])
    data['decile'] = pd.qcut(data['e_p_ratio'], 10, labels=['Dec 1', 'Dec 2', 'Dec 3', 'Dec 4', 'Dec 5', 'Dec 6', 'Dec 7', 'Dec 8', 'Dec 9', 'Dec 10'])
    
    
    return data


def calculate_portfolio_returns(df, portfolio_column='portfolio', weight_column='me', return_column='mthret', date_column='jdate'):
    def weighted_average(x):
        weights = x[weight_column]
        returns = x[return_column]
        total_weight = weights.sum()
        if total_weight > 0:
            return np.average(returns, weights=weights)
        else:
            return np.nan  
    vw_returns = df.groupby([date_column, portfolio_column]).apply(weighted_average).reset_index(name='vw_return')
    
    return vw_returns



crsp['permno'] = pd.to_numeric(crsp['permno'], errors='coerce').astype('Int64')
comp['gvkey'] = pd.to_numeric(comp['gvkey'], errors='coerce').astype('Int64')


crsp = calculate_market_equity(crsp)
crsp, crsp_jun = use_dec_market_equity(crsp)
merged_data = pd.merge(crsp, comp, how='inner', left_on='permno', right_on='gvkey')

portfolios = sort_into_portfolios(merged_data)

portfolio_returns = calculate_portfolio_returns(portfolios)
portfolio_returns.to_csv(OUTPUT_DIR / 'portfolio_returns.csv', index=False)