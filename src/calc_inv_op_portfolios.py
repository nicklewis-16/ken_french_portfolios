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



# def calc_book_equity_and_years_in_compustat(comp):
#     """Calculate book equity and number of years in Compustat.

#     Use load_CRSP_Compustat.description_crsp for help
#     """
#     comp['ps'] = comp['pstkrv'].fillna(comp['pstkl']).fillna(comp['pstk']).fillna(0)
    
#     # Replace null in 'txditc' with 0
#     comp['txditc'] = comp['txditc'].fillna(0)
    
#     # Calculate book equity ('be')
#     comp['be'] = comp['seq'] + comp['txditc'] - comp['ps']
    
#     # Ensure book equity is positive; otherwise, set as NaN
#     comp['be'] = comp['be'].where(comp['be'] > 0)
    
#     # Sort by 'gvkey' and 'datadate' and calculate the number of years in Compustat
#     comp = comp.sort_values(by=["gvkey", "datadate"])
#     comp['count'] = comp.groupby('gvkey').cumcount() 

#     return comp

# def subset_CRSP_to_common_stock_and_exchanges(crsp):
#     """Subset to common stock universe and
#     stocks traded on NYSE, AMEX and NASDAQ.

#     NOTE:
#         With the new CIZ format, it is not necessary to apply delisting
#         returns, as they are already applied.
#     """
#     crsp_filtered = crsp[
#         (crsp['sharetype']=='NS')  &
#         (crsp['securitytype'] == 'EQTY') &  # Confirm this correctly filters equity securities
#         (crsp['securitysubtype'] == 'COM') &  # Ensure 'COM' accurately captures common stocks
#         (crsp['usincflg'] == 'Y') &  # U.S.-incorporated
#         # Assuming more precise issuer types or additional conditions were found
#         (crsp['issuertype'].isin(['ACOR', 'CORP'])) &  # Confirm these are the correct issuer types
#         (crsp['primaryexch'].isin(['N', 'A', 'Q'])) &  
#         (crsp['tradingstatusflg'] == 'A')&
#         (crsp['conditionaltype'] == 'RW') 
#         ]

#     return crsp_filtered

# def calculate_market_equity(crsp):
#     """
#     'There were cases when the same firm (permco) had two or more securities
#     (permno) on the same date. For the purpose of ME for the firm, we
#     aggregated all ME for a given permco, date. This aggregated ME was assigned
#     to the CRSP permno that has the largest ME.
#     """
#     crsp['me']=crsp['mthprc']*crsp['shrout']
#     agg_me = crsp.groupby(['jdate', 'permco'])['me'].sum().reset_index()
#     max_me = crsp.groupby(['jdate', 'permco'])['me'].max().reset_index()
#     crsp = crsp.merge(max_me, how='inner', on=['jdate','permco', 'me'])
#     crsp = crsp.drop('me', axis=1)
#     crsp = crsp.merge(agg_me, how='inner', on=['jdate','permco'])
#     crsp = crsp.sort_values(by=['permno','jdate']).drop_duplicates()
#     return crsp


def use_dec_market_equity(crsp2):
    """
    Finally, ME at June and December
    were flagged since (1) December ME will be used to create Book-to-Market
    ratio (BEME) and (2) June ME has to be positive in order to be part of
    the portfolio.'

    """
    crsp2["year"] = crsp2["date"].dt.year
    crsp2["month_num"] = crsp2["date"].dt.month
    decme = crsp2[crsp2["month_num"] == 12]
    decme = decme[["permno", "month", "date", "mktcap", "year"]].rename(
        columns={"mktcap": "dec_me"}
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
    # crsp2["L_me"] = crsp2.groupby(["permno"])["me"].shift(1)

    # if first permno then use me/(1+retx) to replace the missing value
    crsp2["count"] = crsp2.groupby(["permno"]).cumcount()
    # crsp2["mktcap_lag"] = np.where(
    #     crsp2["count"] == 0, crsp2["mktcap"] / crsp2["1+retx"], crsp2["mktcap_lag"]
    # )

    # baseline me
    mebase = crsp2[crsp2["ffmonth"] == 1][["permno", "ffyear", "mktcap_lag"]].rename(
        columns={"mktcap_lag": "mebase"}
    )

    # merge result back together
    crsp3 = pd.merge(crsp2, mebase, how="left", on=["permno", "ffyear"])
    crsp3["wt"] = np.where(
        crsp3["ffmonth"] == 1, crsp3["mktcap_lag"], crsp3["mebase"] * crsp3["L_cumretx"]
    )

    decme["year"] = decme["year"] + 1
    decme = decme[["permno", "year", "dec_me"]]

    # Info as of June
    crsp3_jun = crsp3[crsp3["month_num"] == 6]

    crsp_jun = pd.merge(crsp3_jun, decme, how="inner", on=["permno", "year"])
    crsp_jun = crsp_jun[
        [
            "permno",
            "month",
            "date",
            # "sharetype",
            # "securitytype",
            # "securitysubtype",
            # "usincflg",
            # "issuertype",
            "exchange",
            # "conditionaltype",
            # "tradingstatusflg",
            "ret",
            "mktcap",
            "wt",
            "cumretx",
            "mebase",
            "mktcap_lag",
            "dec_me",
        ]
    ]
    crsp_jun = crsp_jun.sort_values(by=["permno", "date"]).drop_duplicates()
    return crsp3, crsp_jun


def op_bucket(row):
    """Assign stock to portfolio by op
    """
    if row['opmin'] <= row["year_op"] <= row["op20"]:
        value = "OP1"
    elif row["year_op"] <= row["op40"]:
        value = "OP2"
    elif row["year_op"] <= row["op60"]:
        value = "OP3"
    elif row["year_op"] <= row["op80"]:
        value = "OP4"
    elif row["year_op"] > row["op80"]:
        value = "OP5"
    else:
        value = ""
    return value

def inv_bucket(row):
    """Assign stock to portfolio by inv
    """
    if row['invmin'] <= row["year_inv"] <= row["inv20"]:
        value = "INV1"
    elif row["year_inv"] <= row["inv40"]:
        value = "INV2"
    elif row["year_inv"] <= row["inv60"]:
        value = "INV3"
    elif row["year_inv"] <= row["inv80"]:
        value = "INV4"
    elif row["year_inv"] > row["inv80"]:
        value = "INV5"
    else:
        value = ""
    return value

def merge_CRSP_and_Compustat(crsp_jun, comp, ccm):
    # ccm["date"] = ccm["date"].fillna(pd.to_datetime("today"))
    #comp.rename(columns={"datadate": "date"}, inplace=True)
    comp['month_num'] = comp['datadate'].dt.month
    comp['year'] = comp['datadate'].dt.year
    ccm['month_num'] = ccm['date'].dt.month
    ccm['year'] = ccm['date'].dt.year

    ccm1 = pd.merge(
        comp, ccm, how="left", on=["gvkey", 'year', 'month_num']
    )
    # ccm1["yearend"] = ccm1["datadate"] + YearEnd(0)
    # ccm1["date"] = ccm1["yearend"] + MonthEnd(6)
    # # set link date bounds
    # ccm2 = ccm1[
    #     (ccm1["date"] >= ccm1["date"]) & (ccm1["date"] <= ccm1["date"])
    # ]

    # link comp and crsp
    # ccm_jun = pd.merge(crsp_jun, ccm1, how="inner", on=["permno", "date"])
    ccm_jun = ccm1[ccm1['date'].dt.month == 6]
    #ccm_jun["beme"] = ccm_jun["be"] * 1000 / ccm_jun["dec_me"]
    return ccm_jun

def assign_op_and_inv_portfolios(ccm_jun, crsp3):
    op_df = ccm_jun.groupby(['year', 'permno'])['op'].sum().reset_index().rename(columns={'op': 'year_op'})
    ccm_jun = pd.merge(ccm_jun, op_df, on=['year', 'permno'], how='left')

    inv_df = ccm_jun.groupby(['year', 'permno'])['inv'].sum().reset_index().rename(columns={'inv': 'year_inv'})
    ccm_jun = pd.merge(ccm_jun, inv_df, on=['year', 'permno'], how='left')

    ccm_jun_filtered = ccm_jun[
        ccm_jun['sale'].notnull() & 
        (ccm_jun[['cogs', 'xsga', 'xint']].notnull().any(axis=1))
    ]

    # select NYSE stocks for bucket breakdown, adjusted for new data availability criteria
    nyse = ccm_jun_filtered[
        (ccm_jun_filtered["exchange"] == "NASDAQ") &
        (ccm_jun_filtered["be"] > 0) &
        (ccm_jun_filtered["mktcap"] > 0) #&
        #(ccm_jun_filtered["count"] >= 2)  # ensuring at least 2 years in Compustat, adjusted as per requirement
    ]
    

    # op breakdown
    nyse_op = (
        nyse.groupby(["date"])["year_op"].describe(percentiles=[0, 0.2, 0.4, 0.6, 0.8]).reset_index()
    )
    nyse_op = nyse_op[["date", "0%", "20%", "40%", "60%", "80%"]].rename(
        columns={"0%": 'opmin', "20%": "op20", "40%": "op40", "60%": "op60", "80%": "op80"}
    )

    # inv breakdown
    nyse_inv = (
        nyse.groupby(["date"])["year_inv"].describe(percentiles=[0, 0.2, 0.4, 0.6, 0.8]).reset_index()
    )
    nyse_inv = nyse_inv[["date", "0%", "20%", "40%", "60%", "80%"]].rename(
        columns={"0%": 'invmin', "20%": "inv20", "40%": "inv40", "60%": "inv60", "80%": "inv80"}
    )

    nyse_breaks = pd.merge(nyse_op, nyse_inv, how="inner", on=["date"])

    # join back size and beme breakdown
    ccm1_jun = pd.merge(ccm_jun, nyse_breaks, how="left", on=["date"])

    # assign op portfolio
    ccm1_jun["opport"] = np.where(
        (ccm1_jun["be"] > 0) & (ccm1_jun["mktcap"] > 0), # & (ccm1_jun["count"] >= 1),
        ccm1_jun.apply(op_bucket, axis=1),
        "",
    )

    # assign inv portfolio
    ccm1_jun["invport"] = np.where(
        (ccm1_jun["be"] > 0) & (ccm1_jun["mktcap"] > 0), #& (ccm1_jun["count"] >= 1),
        ccm1_jun.apply(inv_bucket, axis=1),
        "",
    )

    # create positivebmeme and nonmissport variable
    ccm1_jun["posbm"] = np.where(
        (ccm1_jun["be"] > 0) & (ccm1_jun["mktcap"] > 0), 1, 0 #& (ccm1_jun["count"] >= 1), 1, 0
    )
    ccm1_jun["nonmissport"] = np.where((ccm1_jun["invport"] != ""), 1, 0)
        
    # store portfolio assignment as of June

    june = ccm1_jun[
        ["permno", "month", "date", "opport", "invport", "posbm", "nonmissport"]
    ].copy()
    june["ffyear"] = june["date"].dt.year

    # merge back with monthly records
    crsp3 = crsp3[
        [
            "month",
            'year',
            "permno",
            # "sharetype",
            # "securitytype",
            # "usincflg",
            # "issuertype",
            "exchange",
            # "conditionaltype",
            # "tradingstatusflg",
            "ret",
            'retx',
            "mktcap",
            "wt",
            "cumretx",
            "ffyear",
            "date",
        ]
    ]
    ccm3 = pd.merge(
        crsp3,
        june[["permno", "ffyear", "opport", "invport", "posbm", "nonmissport"]],
        how="left",
        on=["permno", "ffyear"],
    )

    # keeping only records that meet the criteria
    ccm4 = ccm3[(ccm3["wt"] > 0) & (ccm3["posbm"] == 1) & (ccm3["nonmissport"] == 1)]
    return ccm4


# def calculate_op_inv(comp):
#     # Adjust OP calculation
#     comp['op'] = (comp['sale'] - comp['cogs'] - comp['xsga'] - comp['xint']) / comp['be'].shift(1)
#     comp['op'] = comp['op'].fillna(0)  # Fill NaN values with 0

#     # Adjust INV calculation
#     comp['inv'] = (comp['at'].shift(1) - comp['at'].shift(2)) / comp['at'].shift(2)
#     comp['inv'] = comp['inv'].fillna(0)  # Fill NaN values with 0

#     return comp


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
    vwret_m["OIport"] = vwret_m["opport"] + vwret_m["invport"]

    # monthly equal-weigthed return
    eq_wavg = (lambda x: np.mean(x['retx']))

    ewret_m = (
        ccm4.groupby(["date", "opport", "invport"])
        .apply(eq_wavg)
        .to_frame()
        .reset_index()
        .rename(columns={0: "ewret_m"})
    )
    ewret_m["OIport"] = ewret_m["opport"] + ewret_m["invport"]

    # yearly value-weigthed return
    vwret_y = (
        ccm4.groupby(["year", "opport", "invport"])
        .apply(wavg, "retx", "wt")
        .to_frame()
        .reset_index()
        .rename(columns={0: "vwret_y"})
    )
    vwret_y["OIport"] = vwret_y["opport"] + vwret_y["invport"]

    # yearly equal-weigthed return
    #eq_wavg = (lambda x: np.mean(x['mthret']))

    ewret_y = (
        ccm4.groupby(["year", "opport", "invport"])
        .apply(eq_wavg)
        .to_frame()
        .reset_index()
        .rename(columns={0: "ewret_y"})
    )
    ewret_y["OIport"] = ewret_y["opport"] + ewret_y["invport"]

    # firm count
    num_firms = (
        ccm4.groupby(["date", "opport", "invport"])["retx"]
        .count()
        .reset_index()
        .rename(columns={"retx": "n_firms"})
    )
    num_firms["OIport"] = num_firms["opport"] + num_firms["invport"]
    num_firms = num_firms.pivot(index="date", columns="OIport", values="n_firms")

    ## market cap
    avg_market_cap = (lambda x: np.mean(x['mktcap']))

    cap = (
        ccm4.groupby(["date", "opport", "invport"])
        .apply(avg_market_cap)
        .to_frame()
        .reset_index()
        .rename(columns={0: "m_cap"})
    )
    cap["OIport"] = cap["opport"] + cap["invport"]

    return vwret_m, ewret_m, vwret_y, ewret_y, num_firms, cap



def create_op_inv_factors(data_dir=DATA_DIR):
        
    ###########################
    ## Load Data
    ###########################

    comp = load_compustat(data_dir=DATA_DIR)
    # crsp = load_CRSP_stock_ciz(data_dir=DATA_DIR)
    crsp = load_CRSP_stock(data_dir=DATA_DIR)
    ccm = load_CRSP_Comp_Link_Table(data_dir=DATA_DIR)
    
    ###########################
    ## Prep Data
    ###########################
    # Prep CRSP and Compustat data according to the Fama-French 1993 
    # methodology described in the document linked in the 
    # module's docstring (at the top of this file).

    # comp = calc_book_equity_and_years_in_compustat(comp)
    # crsp = subset_CRSP_to_common_stock_and_exchanges(crsp)

    # crsp['mthret']=crsp['mthret'].fillna(0)
    # crsp['mthretx']=crsp['mthretx'].fillna(0)
    # crsp2 = calculate_market_equity(crsp)
    crsp3, crsp_jun = use_dec_market_equity(crsp)
    ccm_jun = merge_CRSP_and_Compustat(crsp_jun, comp, ccm)
    ccm_jun2 = calculate_op_inv(ccm_jun)
    

    ############################
    ## Form OP INV Factors
    ############################
    ccm4 = assign_op_and_inv_portfolios(ccm_jun, crsp3) # assign op and inv portfolios
    vwret_m, ewret_m, vwret_y, eqret_y, num_firms, cap = create_op_inv_portfolios(ccm4) # create op_inv_portfolios
    # ff_factors, ff_nfirms = create_factors_from_portfolios(vwret, vwret_n) # create factors from op_inv_portfolios
    # return vwret, vwret_n, ff_factors, ff_nfirms




# def calculate_portfolio_returns(df, portfolio_column='portfolio', weight_column='me', return_column='ret'):
#     # Calculate value-weighted returns
#     vw_returns = df.groupby(['date', portfolio_column]).apply(lambda x: np.average(x[return_column], weights=x[weight_column]))
    
#     # Calculate equal-weighted returns
#     ew_returns = df.groupby(['date', portfolio_column])[return_column].mean()
    
#     return vw_returns, ew_returns





# def handle_missing_data(data):
#     """
#     Handle missing data in the dataset.
#     """
#     pass

# # def calc_book_equity(comp):
# #     """
# #     Calculate book equity in Compustat.
# #     """
# #     comp['ps'] = comp['pstkrv'].fillna(comp['pstkl'])
# #     comp['ps'] = comp['ps'].fillna(comp['pstk'])
# #     comp['ps'] = comp['ps'].fillna(0)
# #     comp['txditc'] = comp['txditc'].fillna(0)
# #     comp['be'] = comp['seq'] + comp['txditc'] - comp['ps']
# #     comp['be'] = comp['be'].where(comp['be'] > 0)

# #     return comp

# def sort_into_quintiles(data, metric):
#     """
#     Sort firms into quintiles based on a given metric.
#     """
#     data['year'] = data['date'].dt.year
#     data['month'] = data['date'].dt.month
#     data['fiscal_year'] = data['date'].dt.year if df['month'] > 6 else df['date'].dt.year - 1

#     # Step 1: Aggregate the data to get yearly numbers 
#     yearly_data = data.groupby(['permno', pd.Grouper(key='month', freq='fiscal_year')]).agg({
#         'revt': 'sum',
#         'cogs': 'sum',
#         'xsga': 'sum',
#         'xint': 'sum',
#         'ceq': 'last',
#         'at': 'last'   #Assuming at and ceq are looking at the past year
#     }).reset_index()

#     yearly_data.rename(columns={'month': 'fiscal_year', 'monthly_revenue': 'annual_revenue'}, inplace=True)

#     # Step 2: Assign quintiles for annual revenue and assets for every separate year
#     yearly_data['profit'] = yearly_data['revt'] - yearly_data['cogs'] - yearly_data['xsga'] - yearly_data['xint']
#     yearly_data['op'] = calculate_op(yearly_data['profit'], yearly_data['ceq'])
#     yearly_data['op_quintile'] = yearly_data.groupby('year')['op'].transform(
#         lambda x: pd.qcut(x, 5, labels=False, duplicates='drop')
#     )

#     yearly_data['investment'] = calculate_inv(yearly_data['at'].shift(1), yearly_data['at'])
#     yearly_data['inv_quintile'] = yearly_data.groupby('year')['investment'].transform(
#         lambda x: pd.qcut(x, 5, labels=False, duplicates='drop')
#     )

#     # Step 3: Merge the quintile information back to the monthly observations DataFrame
#     data = pd.merge(data, yearly_data[['permno', 'fiscal_year', 'op_quintile', 'inv_quintile']], on=['permno', 'fiscal_year'], how='left')

#     return data

# def form_portfolios(data):
#     """
#     Form 25 portfolios based on OP and INV quintiles.
#     """
#     data['op_category'] = data.apply(lambda row: 'OP1' if row['op_quintile'] == 1  else 
#                                        'OP2' if row['op_quintile'] == 2 else
#                                        'OP3' if row['op_quintile'] == 3 else
#                                        'OP4' if row['op_quintile'] == 4 else
#                                        'OP5' , axis=1)
#     data['inv_category'] = data.apply(lambda row: 'INV1' if row['inv_quintile'] == 1  else 
#                                        'INV2' if row['inv_quintile'] == 2 else
#                                        'INV3' if row['inv_quintile'] == 3 else
#                                        'INV4' if row['inv_quintile'] == 4 else
#                                        'INV5' , axis=1)
    
#     return data

# def calculate_portfolio_returns(data):
#     """
#     Calculate returns for each portfolio.
#     """
#     data['period'] = data['date'].dt.to_period('M')
#     data['equal_weighted_return'] = data['mthret'] * df['weight']
#     portfolio_returns = data.groupby(['period', 'op_category', 'inv_category'])['equal_weighted_return'].sum()
#     portfolio_returns = portfolio_returns.unstack(level=['op_category', 'inv_category'])
#     portfolio_returns.index = portfolio_returns.index.strftime('%Y%m')
#     portfolio_returns.columns = ['OP1 INV1', 'OP1 INV2', 'OP1 INV3', 'OP1 INV4', 'OP1 INV5', 
#                                  'OP2 INV1', 'OP2 INV2', 'OP2 INV3', 'OP2 INV4', 'OP2 INV5',
#                                  'OP3 INV1', 'OP3 INV2', 'OP3 INV3', 'OP3 INV4', 'OP3 INV5',
#                                  'OP4 INV1', 'OP4 INV2', 'OP4 INV3', 'OP4 INV4', 'OP4 INV5',
#                                  'OP5 INV1', 'OP5 INV2', 'OP5 INV3', 'OP5 INV4', 'OP5 INV5']
#     portfolio_returns = portfolio_returns[['OP1 INV1', 'OP1 INV2', 'OP1 INV3', 'OP1 INV4', 'OP1 INV5', 
#                                  'OP2 INV1', 'OP2 INV2', 'OP2 INV3', 'OP2 INV4', 'OP2 INV5',
#                                  'OP3 INV1', 'OP3 INV2', 'OP3 INV3', 'OP3 INV4', 'OP3 INV5',
#                                  'OP4 INV1', 'OP4 INV2', 'OP4 INV3', 'OP4 INV4', 'OP4 INV5',
#                                  'OP5 INV1', 'OP5 INV2', 'OP5 INV3', 'OP5 INV4', 'OP5 INV5',]]
#     portfolio_returns.reset_index(inplace=True)
#     portfolio_returns.rename(columns={'index': 'period', 'OP1 INV1': 'LoOP LoINV', 'OP1 INV5': 'LoOP HiINV',
#                                       'OP5 INV1': 'HiOP LoINV', 'OP5 INV5': 'HiOP HiINV'}, inplace=True)
#     cols = ['period'] + [col for col in portfolio_returns.columns if col != 'period']
#     portfolio_returns = portfolio_returns[cols]

#     return portfolio_returns



# ### BRYCE FUNCTION IDEAS ###


# # def calc_book_equity_and_years_in_compustat(comp):
# #     """
# #     Calculate book equity in Compustat
# #     """

# # def subset_CRSP_to_common_stock_and_exchanges(crsp):
# #     """
# #     Subset to common stock universe and stocks traded on NYSE, AMEX and NASDAQ.
# #     """

# # def subset_CRSP_data(df):
# #     """
# #     Subset the CRSP data to only include stocks with positive BE, 
# #     total assets data for t-2 and t-1, 
# #     non-missing revenues data for t-1, 
# #     and non-missing data for at least one of the following: 
# #         cost of goods sold, selling, general and administrative expenses, or interest expense for t-1.
# #     """

# # def calc_operating_profitability(df):
# #     """
# #     Calculate operating profitability: annual revenues minus cost of goods sold, interest expense, and selling, general, and administrative expenses 
# #     divided by book equity for the last fiscal year end in t-1.
# #     """

# # def assign_profitability_quintile(df):
# #     """
# #     Assigns a profitability quintile to each stock based on the operating profitability (1-5).
# #     """

# # def calc_investment(df):
# #     """
# #     Calculate investment: the change in total assets from t-2 to t-1 divided by t-2 assets.
# #     """

# # def assign_investment_quintile(df):
# #     """
# #     Assigns an investment quintile to each stock based on the investment (1-5).
# #     """

# # def calc_equal_weighted_index(df):
# #     """
# #     Calculate equal weighted index (just the average of all stocks)
# #     Do this for each double sorted portfolio (25 columns)
# #     """

# # def calc_value_weighted_index(df):
# #     """
# #     Calculate value weighted index (just the average of all stocks)
# #     Do this for each double sorted portfolio (25 columns)
# #     """