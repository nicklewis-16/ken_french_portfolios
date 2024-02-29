import pandas as pd
import numpy as np
from pathlib import Path
import config


OUTPUT_DIR = Path(config.OUTPUT_DIR)
DATA_DIR = Path(config.DATA_DIR)

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


comp = load_compustat(data_dir=DATA_DIR)
crsp = load_CRSP_stock_ciz(data_dir=DATA_DIR)
ccm = load_CRSP_Comp_Link_Table(data_dir=DATA_DIR)


def calculate_op_inv(comp):
    # Operating Profitability (OP)
    comp['op'] = (comp['sale'] - comp['cogs'] - comp['xsga'] - comp['xint']) / comp['at'].shift(1)
    
    # Investment (INV)
    comp['inv'] = (comp['at'] - comp['at'].shift(1)) / comp['at'].shift(2)
    
    return comp


def assign_portfolios(df, date_column='datadate', op_column='op', inv_column='inv'):
    # Ensure the dataframe is sorted by date
    df = df.sort_values(by=[date_column])
    
    # Assign quintiles for OP and INV
    df['op_quintile'] = df.groupby(date_column)[op_column].transform(lambda x: pd.qcut(x, 5, labels=False, duplicates='drop'))
    df['inv_quintile'] = df.groupby(date_column)[inv_column].transform(lambda x: pd.qcut(x, 5, labels=False, duplicates='drop'))
    
    # Create a composite key for the 25 portfolios
    df['portfolio'] = df['op_quintile'].astype(str) + df['inv_quintile'].astype(str)
    
    return df

def calculate_portfolio_returns(df, portfolio_column='portfolio', weight_column='me', return_column='ret'):
    # Calculate value-weighted returns
    vw_returns = df.groupby(['date', portfolio_column]).apply(lambda x: np.average(x[return_column], weights=x[weight_column]))
    
    # Calculate equal-weighted returns
    ew_returns = df.groupby(['date', portfolio_column])[return_column].mean()
    
    return vw_returns, ew_returns





def handle_missing_data(data):
    """
    Handle missing data in the dataset.
    """
    pass

# def calc_book_equity(comp):
#     """
#     Calculate book equity in Compustat.
#     """
#     comp['ps'] = comp['pstkrv'].fillna(comp['pstkl'])
#     comp['ps'] = comp['ps'].fillna(comp['pstk'])
#     comp['ps'] = comp['ps'].fillna(0)
#     comp['txditc'] = comp['txditc'].fillna(0)
#     comp['be'] = comp['seq'] + comp['txditc'] - comp['ps']
#     comp['be'] = comp['be'].where(comp['be'] > 0)

#     return comp

def sort_into_quintiles(data, metric):
    """
    Sort firms into quintiles based on a given metric.
    """
    data['year'] = data['date'].dt.year
    data['month'] = data['date'].dt.month
    data['fiscal_year'] = data['date'].dt.year if df['month'] > 6 else df['date'].dt.year - 1

    # Step 1: Aggregate the data to get yearly numbers 
    yearly_data = data.groupby(['permno', pd.Grouper(key='month', freq='fiscal_year')]).agg({
        'revt': 'sum',
        'cogs': 'sum',
        'xsga': 'sum',
        'xint': 'sum',
        'ceq': 'last',
        'at': 'last'   #Assuming at and ceq are looking at the past year
    }).reset_index()

    yearly_data.rename(columns={'month': 'fiscal_year', 'monthly_revenue': 'annual_revenue'}, inplace=True)

    # Step 2: Assign quintiles for annual revenue and assets for every separate year
    yearly_data['profit'] = yearly_data['revt'] - yearly_data['cogs'] - yearly_data['xsga'] - yearly_data['xint']
    yearly_data['op'] = calculate_op(yearly_data['profit'], yearly_data['ceq'])
    yearly_data['op_quintile'] = yearly_data.groupby('year')['op'].transform(
        lambda x: pd.qcut(x, 5, labels=False, duplicates='drop')
    )

    yearly_data['investment'] = calculate_inv(yearly_data['at'].shift(1), yearly_data['at'])
    yearly_data['inv_quintile'] = yearly_data.groupby('year')['investment'].transform(
        lambda x: pd.qcut(x, 5, labels=False, duplicates='drop')
    )

    # Step 3: Merge the quintile information back to the monthly observations DataFrame
    data = pd.merge(data, yearly_data[['permno', 'fiscal_year', 'op_quintile', 'inv_quintile']], on=['permno', 'fiscal_year'], how='left')

    return data

def form_portfolios(data):
    """
    Form 25 portfolios based on OP and INV quintiles.
    """
    data['op_category'] = data.apply(lambda row: 'OP1' if row['op_quintile'] == 1  else 
                                       'OP2' if row['op_quintile'] == 2 else
                                       'OP3' if row['op_quintile'] == 3 else
                                       'OP4' if row['op_quintile'] == 4 else
                                       'OP5' , axis=1)
    data['inv_category'] = data.apply(lambda row: 'INV1' if row['inv_quintile'] == 1  else 
                                       'INV2' if row['inv_quintile'] == 2 else
                                       'INV3' if row['inv_quintile'] == 3 else
                                       'INV4' if row['inv_quintile'] == 4 else
                                       'INV5' , axis=1)
    
    return data

def calculate_portfolio_returns(data):
    """
    Calculate returns for each portfolio.
    """
    data['period'] = data['date'].dt.to_period('M')
    data['equal_weighted_return'] = data['mthret'] * df['weight']
    portfolio_returns = data.groupby(['period', 'op_category', 'inv_category'])['equal_weighted_return'].sum()
    portfolio_returns = portfolio_returns.unstack(level=['op_category', 'inv_category'])
    portfolio_returns.index = portfolio_returns.index.strftime('%Y%m')
    portfolio_returns.columns = ['OP1 INV1', 'OP1 INV2', 'OP1 INV3', 'OP1 INV4', 'OP1 INV5', 
                                 'OP2 INV1', 'OP2 INV2', 'OP2 INV3', 'OP2 INV4', 'OP2 INV5',
                                 'OP3 INV1', 'OP3 INV2', 'OP3 INV3', 'OP3 INV4', 'OP3 INV5',
                                 'OP4 INV1', 'OP4 INV2', 'OP4 INV3', 'OP4 INV4', 'OP4 INV5',
                                 'OP5 INV1', 'OP5 INV2', 'OP5 INV3', 'OP5 INV4', 'OP5 INV5']
    portfolio_returns = portfolio_returns[['OP1 INV1', 'OP1 INV2', 'OP1 INV3', 'OP1 INV4', 'OP1 INV5', 
                                 'OP2 INV1', 'OP2 INV2', 'OP2 INV3', 'OP2 INV4', 'OP2 INV5',
                                 'OP3 INV1', 'OP3 INV2', 'OP3 INV3', 'OP3 INV4', 'OP3 INV5',
                                 'OP4 INV1', 'OP4 INV2', 'OP4 INV3', 'OP4 INV4', 'OP4 INV5',
                                 'OP5 INV1', 'OP5 INV2', 'OP5 INV3', 'OP5 INV4', 'OP5 INV5',]]
    portfolio_returns.reset_index(inplace=True)
    portfolio_returns.rename(columns={'index': 'period', 'OP1 INV1': 'LoOP LoINV', 'OP1 INV5': 'LoOP HiINV',
                                      'OP5 INV1': 'HiOP LoINV', 'OP5 INV5': 'HiOP HiINV'}, inplace=True)
    cols = ['period'] + [col for col in portfolio_returns.columns if col != 'period']
    portfolio_returns = portfolio_returns[cols]

    return portfolio_returns



### BRYCE FUNCTION IDEAS ###


# def calc_book_equity_and_years_in_compustat(comp):
#     """
#     Calculate book equity in Compustat
#     """

# def subset_CRSP_to_common_stock_and_exchanges(crsp):
#     """
#     Subset to common stock universe and stocks traded on NYSE, AMEX and NASDAQ.
#     """

# def subset_CRSP_data(df):
#     """
#     Subset the CRSP data to only include stocks with positive BE, 
#     total assets data for t-2 and t-1, 
#     non-missing revenues data for t-1, 
#     and non-missing data for at least one of the following: 
#         cost of goods sold, selling, general and administrative expenses, or interest expense for t-1.
#     """

# def calc_operating_profitability(df):
#     """
#     Calculate operating profitability: annual revenues minus cost of goods sold, interest expense, and selling, general, and administrative expenses 
#     divided by book equity for the last fiscal year end in t-1.
#     """

# def assign_profitability_quintile(df):
#     """
#     Assigns a profitability quintile to each stock based on the operating profitability (1-5).
#     """

# def calc_investment(df):
#     """
#     Calculate investment: the change in total assets from t-2 to t-1 divided by t-2 assets.
#     """

# def assign_investment_quintile(df):
#     """
#     Assigns an investment quintile to each stock based on the investment (1-5).
#     """

# def calc_equal_weighted_index(df):
#     """
#     Calculate equal weighted index (just the average of all stocks)
#     Do this for each double sorted portfolio (25 columns)
#     """

# def calc_value_weighted_index(df):
#     """
#     Calculate value weighted index (just the average of all stocks)
#     Do this for each double sorted portfolio (25 columns)
#     """