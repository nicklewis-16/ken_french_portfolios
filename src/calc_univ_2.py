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
    decme = crsp2[crsp2["month"] == 12].copy()
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

    #decme.loc[:, "year"] = decme["year"] - 1
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

    ccm1 = pd.merge(comp, ccm, how="left", on=["gvkey"])
    ccm1["yearend"] = ccm1["datadate"] + YearEnd(0)
    ccm1["jdate"] = ccm1["yearend"] + MonthEnd(6)

    ccm2 = ccm1[(ccm1["jdate"] >= ccm1["linkdt"]) & (ccm1["jdate"] <= ccm1["linkenddt"])]

    ccm_jun = pd.merge(crsp_jun, ccm2, how="inner", on=["permno", "jdate"])
    ccm_jun["ep"] = ccm_jun["ni"] * 1000 / ccm_jun["dec_me"]
    # Add calculations for cf and cfp
    ccm_jun['cf'] = ccm_jun['ebit'] + ccm_jun['dp'].fillna(0) + ccm_jun['txditc'].fillna(0)
    ccm_jun['cfp'] = ccm_jun['cf'] / ccm_jun['dec_me']
    return ccm_jun


comp = load_compustat(data_dir=DATA_DIR)
crsp = load_CRSP_stock_ciz(data_dir=DATA_DIR)
ccm = load_CRSP_Comp_Link_Table(data_dir=DATA_DIR)


crsp[['permco', 'permno']] = crsp[['permco', 'permno']].astype(int)
crsp['jdate'] = crsp['mthcaldt'] + MonthEnd(0)
crsp['mthretx'] = pd.to_numeric(crsp['mthretx'], errors='coerce')
crsp['mthret'] = pd.to_numeric(crsp['mthret'], errors='coerce')
crsp['year'] = crsp['mthcaldt'].dt.year

annual_ret_ex_div = crsp.groupby(['permno', 'year'])['mthretx'].apply(lambda x: (1 + x).prod() - 1).reset_index(name='annual_ret_ex_div')
annual_ret_inc_div = crsp.groupby(['permno', 'year'])['mthret'].apply(lambda x: (1 + x).prod() - 1).reset_index(name='annual_ret_inc_div')

crsp = pd.merge(crsp, annual_ret_ex_div, on=['permno', 'year'], how='left')
crsp = pd.merge(crsp, annual_ret_inc_div, on=['permno', 'year'], how='left')


crsp = subset_CRSP_to_common_stock_and_exchanges(crsp)
crsp2 = calculate_market_equity(crsp)
crsp3, crsp_jun = use_dec_market_equity(crsp2)
ccm_jun = merge_CRSP_and_Compustat(crsp_jun, comp, ccm)

def categorize_metric_exclusive(row, metric, bottom_30_bp, top_30_bp, quintiles_bp, deciles_bp):
    """
    Determine categories for a given metric value within a row.
    This is a placeholder function. Replace its internals based on actual categorization logic.
    """
    value = row[metric]
    categories = []

    if value < 0:
        categories.append('<=0')
    elif value <= bottom_30_bp:
        categories.append('Lo 30')
    elif value <= top_30_bp:
        categories.append('Med 40')
    else:
        categories.append('Hi 30')

    quintiles = np.searchsorted(quintiles_bp, value, side='right')
    categories.append(f'Qnt {quintiles+1}')

    deciles = np.searchsorted(deciles_bp, value, side='right')
    categories.append(f'Dec {deciles+1}')

    return categories

def categorize_stocks_by_metric(dataframe, metric, category_name):
    """
    Categorize stocks by a specified metric and directly append the categories to the dataframe.
    """
    # Calculate breakpoints for the NYSE stocks each year to define categories
    for year, group in dataframe.groupby('year'):
        non_negative = group[group[metric] >= 0]
        negative = group[group[metric] < 0]

        sorted_values = non_negative[metric].sort_values()

        bottom_30_bp = sorted_values.quantile(q=0.3)
        top_30_bp = sorted_values.quantile(q=0.7)
        quintiles_bp = sorted_values.quantile(q=np.arange(0.2, 1.01, 0.2))
        deciles_bp = sorted_values.quantile(q=np.arange(0.1, 1.01, 0.1))

        # Assign categories based on calculated breakpoints
        dataframe.loc[non_negative.index, category_name] = non_negative.apply(
            lambda row: categorize_metric_exclusive(row, metric, bottom_30_bp, top_30_bp, quintiles_bp.values, deciles_bp.values), axis=1)
        dataframe.loc[negative.index, category_name] = [['Negative Values']] * len(negative)

    return dataframe


categorize_stocks_by_metric(ccm_jun, 'ep', 'ep_categories')
categorize_stocks_by_metric(ccm_jun, 'cfp', 'cfp_categories')

def update_portfolio_assignments(df, category_field, portfolio_prefix):
    """
    Update the dataframe with portfolio assignments based on the categories.

    Args:
    - df: DataFrame to be updated.
    - category_field: Field name containing the categories.
    - portfolio_prefix: Prefix for the portfolio names.
    """
    # Explode the DataFrame to work with one category per row
    df_exploded = df.explode(category_field)

    # Initialize portfolio columns to NaN
    unique_categories = df_exploded[category_field].dropna().unique()
    for category in unique_categories:
        portfolio_name = f"{portfolio_prefix}_{category}"
        df[portfolio_name] = np.nan

    for index, row in df_exploded.dropna(subset=[category_field]).iterrows():
        category = row[category_field]
        portfolio_name = f"{portfolio_prefix}_{category}"
        df.at[index, portfolio_name] = row['permno']

    return df


ccm_jun = update_portfolio_assignments(ccm_jun, 'ep_categories', 'ep')
ccm_jun = update_portfolio_assignments(ccm_jun, 'cfp_categories', 'cfp')

def calculate_portfolio_returns(df, portfolio_prefix):
    portfolio_returns = {}
    for column in df.columns:
        if column.startswith(portfolio_prefix):
            if column in df and pd.api.types.is_numeric_dtype(df[column]):
                # Calculate weighted average return for each group
                weighted_avg = df.groupby('jdate').apply(
                    lambda x: np.average(x['mthret'].fillna(0), weights=x[column].fillna(0))
                    if not x[column].isnull().all() else np.nan
                )
                portfolio_returns[column] = weighted_avg
            else:
                print(f"Skipping {column} due to non-numeric or missing data")
    
    return pd.DataFrame(portfolio_returns)


portfolio_returns_ep = calculate_portfolio_returns(ccm_jun, 'ep')
portfolio_returns_cfp = calculate_portfolio_returns(ccm_jun, 'cfp')

def calculate_portfolio_monthly_returns(df, metric_categories):
    # Calculate value-weighted returns
    df['weight'] = df['me'] / df.groupby(['jdate', metric_categories])['me'].transform('sum')
    df['weighted_ret'] = df['mthret'] * df['weight']
    value_weighted = df.groupby(['jdate', metric_categories])['weighted_ret'].sum().reset_index(name='value_weighted_ret')

    # Calculate equal-weighted returns
    df['equal_weight'] = 1 / df.groupby(['jdate', metric_categories])['permno'].transform('count')
    df['equal_weighted_ret'] = df['mthret'] * df['equal_weight']
    equal_weighted = df.groupby(['jdate', metric_categories])['equal_weighted_ret'].sum().reset_index(name='equal_weighted_ret')

    return value_weighted, equal_weighted

def calculate_portfolio_annual_returns(df, metric_categories):
    df['annual_ret'] = df.groupby(['permno', 'year'])['mthret'].transform(lambda x: (1 + x).prod() - 1)
    # Value-weighted
    df['annual_weight'] = df['me'] / df.groupby(['year', metric_categories])['me'].transform('sum')
    df['annual_weighted_ret'] = df['annual_ret'] * df['annual_weight']
    value_weighted_annual = df.groupby(['year', metric_categories])['annual_weighted_ret'].sum().reset_index(name='value_weighted_annual_ret')

    # Equal-weighted
    equal_weighted_annual = df.groupby(['year', metric_categories])['annual_ret'].mean().reset_index(name='equal_weighted_annual_ret')

    return value_weighted_annual, equal_weighted_annual


ccm_jun['ep_categories'] = ccm_jun['ep_categories'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
ccm_jun['cfp_categories'] = ccm_jun['cfp_categories'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)


value_weighted_ep, equal_weighted_ep = calculate_portfolio_monthly_returns(ccm_jun, 'ep_categories')
value_weighted_cfp, equal_weighted_cfp = calculate_portfolio_monthly_returns(ccm_jun, 'cfp_categories')

value_weighted_annual_ep, equal_weighted_annual_ep = calculate_portfolio_annual_returns(ccm_jun, 'ep_categories')
value_weighted_annual_cfp, equal_weighted_annual_cfp = calculate_portfolio_annual_returns(ccm_jun, 'cfp_categories')

def calculate_firm_size_and_count(df, metric_categories):
    average_firm_size = df.groupby(['year', metric_categories])['me'].mean().reset_index(name='average_me')
    number_of_firms = df.groupby(['year', metric_categories]).size().reset_index(name='num_firms').rename(columns={0: 'count'})
    return average_firm_size, number_of_firms

average_size_ep, firm_count_ep = calculate_firm_size_and_count(ccm_jun, 'ep_categories')
average_size_cfp, firm_count_cfp = calculate_firm_size_and_count(ccm_jun, 'cfp_categories')


with pd.ExcelWriter(OUTPUT_DIR / 'portfolio_metrics.xlsx') as writer:
    value_weighted_ep.to_excel(writer, sheet_name='Value Weighted Monthly EP')
    equal_weighted_ep.to_excel(writer, sheet_name='Equal Weighted Monthly EP')
    value_weighted_cfp.to_excel(writer, sheet_name='Value Weighted Monthly CFP')
    equal_weighted_cfp.to_excel(writer, sheet_name='Equal Weighted Monthly CFP')
    
    value_weighted_annual_ep.to_excel(writer, sheet_name='Value Weighted Annual EP')
    equal_weighted_annual_ep.to_excel(writer, sheet_name='Equal Weighted Annual EP')
    value_weighted_annual_cfp.to_excel(writer, sheet_name='Value Weighted Annual CFP')
    equal_weighted_annual_cfp.to_excel(writer, sheet_name='Equal Weighted Annual CFP')
    
    average_size_ep.to_excel(writer, sheet_name='Average Size EP')
    firm_count_ep.to_excel(writer, sheet_name='Firm Count EP')
    average_size_cfp.to_excel(writer, sheet_name='Average Size CFP')
    firm_count_cfp.to_excel(writer, sheet_name='Firm Count CFP')