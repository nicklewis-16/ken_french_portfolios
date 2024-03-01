import pandas as pd
import numpy as np
from pandas.tseries.offsets import MonthEnd, YearEnd
from pathlib import Path
import config
from load_CRSP_Compustat import *
from load_CRSP_stock import *

OUTPUT_DIR = Path(config.OUTPUT_DIR)
DATA_DIR = Path(config.DATA_DIR)


def subset_CRSP_to_common_stock_and_exchanges(crsp):
    """Subset to common stock universe and
    stocks traded on NYSE, AMEX and NASDAQ.

    NOTE:
        With the new CIZ format, it is not necessary to apply delisting
        returns, as they are already applied.
    """
    crsp_filtered = crsp[
        (crsp['sharetype'] == 'NS') &
        (crsp['securitytype'] == 'EQTY') &  # Confirm this correctly filters equity securities
        (crsp['securitysubtype'] == 'COM') &  # Ensure 'COM' accurately captures common stocks
        (crsp['usincflg'] == 'Y') &  # U.S.-incorporated
        # Assuming more precise issuer types or additional conditions were found
        (crsp['issuertype'].isin(['ACOR', 'CORP'])) &  # Confirm these are the correct issuer types
        (crsp['primaryexch'].isin(['N', 'A', 'Q'])) &
        (crsp['tradingstatusflg'] == 'A') &
        (crsp['conditionaltype'] == 'RW')
        ]

    return crsp_filtered


def calculate_market_equity(crsp):
    """
    'There were cases when the same firm (permco) had two or more securities
    (permno) on the same date. For the purpose of ME for the firm, we
    aggregated all ME for a given permco, date. This aggregated ME was assigned
    to the CRSP permno that has the largest ME.
    """
    crsp['me'] = crsp['mthprc'] * crsp['shrout']
    agg_me = crsp.groupby(['jdate', 'permco'])['me'].sum().reset_index()
    max_me = crsp.groupby(['jdate', 'permco'])['me'].max().reset_index()
    crsp = crsp.merge(max_me, how='inner', on=['jdate', 'permco', 'me'])
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
    decme["year"] = decme["year"] - 1 #Shift ME to align with December of t-1
    decme = decme[["permno", "mthcaldt", "jdate", "me", "year"]].rename(
        columns={"me": "dec_me"}
    )

    ### July to June dates
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


def merge_CRSP_and_Compustat(crsp_jun, comp, ccm):
    ccm["linkenddt"] = ccm["linkenddt"].fillna(pd.to_datetime("today"))

    ccm1 = pd.merge(
        comp, ccm, how="left", on=["gvkey"]
    )
    ccm1["yearend"] = ccm1["datadate"] + YearEnd(0)
    ccm1["jdate"] = ccm1["yearend"] + MonthEnd(6)
    # set link date bounds
    ccm2 = ccm1[
        (ccm1["jdate"] >= ccm1["linkdt"]) & (ccm1["jdate"] <= ccm1["linkenddt"])
    ]

    # link comp and crsp
    ccm_jun = pd.merge(crsp_jun, ccm2, how="inner", on=["permno", "jdate"])
    ccm_jun["ep"] = ccm_jun["ni"] * 1000 / ccm_jun["dec_me"]
    return ccm_jun


comp = load_compustat(data_dir=DATA_DIR)
crsp = load_CRSP_stock_ciz(data_dir=DATA_DIR)
ccm = load_CRSP_Comp_Link_Table(data_dir=DATA_DIR)


crsp = subset_CRSP_to_common_stock_and_exchanges(crsp)
crsp2 = calculate_market_equity(crsp)
crsp3, crsp_jun = use_dec_market_equity(crsp2)
ccm_jun = merge_CRSP_and_Compustat(crsp_jun, comp, ccm)

def categorize_ep_exclusive(row, bottom_30_bp, top_30_bp, quintiles_bp, deciles_bp):
    categories = []
    
    if row['ep'] < 0:
        return ['Negative Values']
    
    if row['ep'] <= bottom_30_bp:
        categories.append('Bottom 30%')
    elif row['ep'] <= top_30_bp:
        if row['ep'] > bottom_30_bp:
            categories.append('Mid 40%')
    else:
        categories.append('Top 30%')
    
    for idx, q_bp in enumerate(quintiles_bp):
        if row['ep'] <= q_bp:
            categories.append(f'Quintile {idx + 1}')
            break
    
    for idx, d_bp in enumerate(deciles_bp):
        if row['ep'] <= d_bp:
            categories.append(f'Decile {idx + 1}')
            break
            
    return categories

nyse_stocks = ccm_jun[ccm_jun['primaryexch'] == 'N']
categorized_data_exclusive = []

for year, group in nyse_stocks.groupby('year'):
    non_negative = group[group['ep'] >= 0].copy()  # Separate non-negative e/p values
    negative = group[group['ep'] < 0].copy()  # Separate negative e/p values
    
    sorted_non_negative = non_negative.sort_values(by='ep')
    
    bottom_30_bp = sorted_non_negative['ep'].quantile(q=0.3)
    top_30_bp = sorted_non_negative['ep'].quantile(q=0.7)
    quintiles_bp = sorted_non_negative['ep'].quantile(q=np.arange(0.2, 1.01, 0.2)).values
    deciles_bp = sorted_non_negative['ep'].quantile(q=np.arange(0.1, 1.01, 0.1)).values
    
    sorted_non_negative['categories'] = sorted_non_negative.apply(categorize_ep_exclusive, axis=1,
                                                                   bottom_30_bp=bottom_30_bp, top_30_bp=top_30_bp,
                                                                   quintiles_bp=quintiles_bp, deciles_bp=deciles_bp)
    
    negative['categories'] = [['Negative Values'] for _ in range(len(negative))]
    
    categorized_data_exclusive.extend(sorted_non_negative.to_dict('records'))
    categorized_data_exclusive.extend(negative.to_dict('records'))

categorized_df_exclusive = pd.DataFrame(categorized_data_exclusive)


portfolio_categories = ['Negative Values', 'Bottom 30%', 'Mid 40%', 'Top 30%'] + \
                       [f'Quintile {i+1}' for i in range(5)] + [f'Decile {i+1}' for i in range(10)]

yearly_portfolios = {year: {category: [] for category in portfolio_categories} for year in ccm_jun['year'].unique()}

def update_portfolios(row):
    year = row['year']
    for category in row['categories']:
        yearly_portfolios[year][category].append(row['permno'])

categorized_df_exclusive.apply(update_portfolios, axis=1)

for year in yearly_portfolios:
    for category in yearly_portfolios[year]:
        yearly_portfolios[year][category] = list(set(yearly_portfolios[year][category]))

yearly_portfolios_example = yearly_portfolios[next(iter(yearly_portfolios))]



crsp_df_cleaned = crsp[['permno', 'mthcaldt', 'mthret', 'me']].dropna(subset=['mthret', 'me'])
crsp_df_cleaned['mthcaldt'] = pd.to_datetime(crsp_df_cleaned['mthcaldt'])
crsp_df_cleaned = crsp_df_cleaned.fillna(0)


portfolio_returns = pd.DataFrame(columns=portfolio_categories)


portfolio_assignments = pd.DataFrame([
    {"year": year, "permno": permno, "portfolio": portfolio}
    for year, portfolios in yearly_portfolios.items()
    for portfolio, permnos in portfolios.items()
    for permno in permnos
])


crsp_with_portfolios = pd.merge(crsp_df_cleaned, portfolio_assignments, on='permno', how='left')

crsp_with_portfolios['year'] = crsp_with_portfolios['mthcaldt'].dt.year

me_sums = crsp_with_portfolios.groupby(['mthcaldt', 'portfolio'])['me'].sum().reset_index(name='me_sum')

crsp_with_portfolios = pd.merge(crsp_with_portfolios, me_sums, on=['mthcaldt', 'portfolio'], how='left')

crsp_with_portfolios['weight'] = crsp_with_portfolios['me'] / crsp_with_portfolios['me_sum']

crsp_with_portfolios['weighted_ret'] = crsp_with_portfolios['mthret'] * crsp_with_portfolios['weight']

portfolio_monthly_returns = crsp_with_portfolios.groupby(['mthcaldt', 'portfolio'])['weighted_ret'].sum().unstack()

portfolio_monthly_returns.index.name = 'Date'

name_mapping = {
    'Negative Values': '<=0',
    'Bottom 30%': 'Lo 30',
    'Mid 40%': 'Med 40',
    'Top 30%': 'Hi 30',
    'Quintile 1': 'Lo 20',
    'Quintile 2': 'Qnt 2',
    'Quintile 3': 'Qnt 3',
    'Quintile 4': 'Qnt 4',
    'Quintile 5': 'Hi 20',
    'Decile 1': 'Lo 10',
    'Decile 2': '2 Dec',
    'Decile 3': '3 Dec',
    'Decile 4': '4 Dec',
    'Decile 5': '5 Dec',
    'Decile 6': '6 Dec',
    'Decile 7': '7 Dec',
    'Decile 8': '8 Dec',
    'Decile 9': '9 Dec',
    'Decile 10': 'Hi 10'
}

new_order = ['<=0', 'Lo 30', 'Med 40', 'Hi 30', 'Lo 20', 'Qnt 2', 'Qnt 3', 'Qnt 4', 'Hi 20', 'Lo 10', '2 Dec', '3 Dec', '4 Dec', '5 Dec', '6 Dec', '7 Dec', '8 Dec', '9 Dec', 'Hi 10']

portfolio_monthly_returns_renamed = portfolio_monthly_returns.rename(columns=name_mapping)

portfolio_monthly_returns_final = portfolio_monthly_returns_renamed[new_order]

portfolio_monthly_returns_final.to_excel(OUTPUT_DIR / 'portfolio_vw_returns.xlsx')

#data to be filtered out using START_DATE and END_DATE from config.py