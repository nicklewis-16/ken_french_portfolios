import pandas as pd
import numpy as np
from pathlib import Path
import config
from pull_raw_data import *


OUTPUT_DIR = Path(config.OUTPUT_DIR)
DATA_DIR = Path(config.DATA_DIR)


def load_financial_data(filepath):
    """
    Load financial data from a given filepath.
    """
    pass

def calculate_op(profit, total_assets):
    """
    Calculate operating profitability (OP).
    """
    pass

def calculate_inv(asset_previous, asset_current):
    """
    Calculate investment (INV) growth rate.
    """
    pass

def handle_missing_data(data):
    """
    Handle missing data in the dataset.
    """
    pass

def sort_into_quintiles(data, metric):
    """
    Sort firms into quintiles based on a given metric.
    """
    pass

def form_portfolios(data):
    """
    Form 25 portfolios based on OP and INV quintiles.
    """
    pass

def calculate_portfolio_returns(portfolios):
    """
    Calculate returns for each portfolio.
    """
    pass



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