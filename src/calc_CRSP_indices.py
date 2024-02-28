"""
Thank you to Tobias Rodriguez del Pozo for his assistance in writing this code.
"""
import pandas as pd
import numpy as np
import config

OUTPUT_DIR = config.OUTPUT_DIR
DATA_DIR = config.DATA_DIR

import misc_tools
import load_CRSP_stock


def calc_equal_weighted_index(df):
    """
    Calculate equal weighted index (just the average of all stocks)
    Note that ret is raw and retx is adjusted for dividends.
    """
    #  Group by date and calculate the mean of returns for each date
    df_eq_idx = df.groupby('date').agg({'ret': 'mean', 'retx': 'mean'}).rename(columns={'ret': 'ewretd', 'retx': 'ewretx'})
    # Correctly count the number of securities for each date
    df_eq_idx['totcnt'] = df.groupby('date')['permno'].nunique()
    return df_eq_idx


def calc_CRSP_value_weighted_index(df):
    """
    The formula is:
    $$
    r_t = \\frac{\\sum_{i=1}^{N_t} w_{i,t-1} r_{i,t}}{\\sum_{i=1}^{N_t} w_{i,t-1}}
    $$
    That is, the return of the index is the weighted average of the returns, where
    the weights are the market cap of the stock at the end of the previous month.
    """
    # Ensure data is sorted by date for correct lagging
    df.sort_values(by=['permno', 'date'], inplace=True)

    # Calculate market cap
    df['mktcap'] = df['altprc'] * df['shrout']
    
    # Lag market cap to get previous month's market cap
    df['prev_mktcap'] = df.groupby('permno')['mktcap'].shift(1)

    # Drop NA values that result from shifting market cap
    df.dropna(subset=['prev_mktcap'], inplace=True)

    # Calculate weighted returns
    df['weighted_ret'] = df['ret'] * df['prev_mktcap']
    df['weighted_retx'] = df['retx'] * df['prev_mktcap']
    
    sum_df = df.groupby('date').agg({
        'weighted_ret': 'sum',
        'weighted_retx': 'sum',
        'prev_mktcap': 'sum'
    })

    # Calculate value-weighted returns
    sum_df['vwretd'] = sum_df['weighted_ret'] / sum_df['prev_mktcap']
    sum_df['vwretx'] = sum_df['weighted_retx'] / sum_df['prev_mktcap']
    # total market cap for each date
    total_mktcap = df.groupby('date')['mktcap'].sum()
    sum_df['totval'] = total_mktcap
    sum_df.reset_index(inplace=True)

    # Ensure the DataFrame matches the expected columns and order
    result_df = sum_df[['date', 'vwretd', 'vwretx', 'totval']].copy()
    result_df.set_index('date', inplace=True)

    return result_df


def calc_CRSP_indices_merge(df_msf, df_msix):
    # Merge everything with appropriate suffixes
    df_vw_idx = calc_CRSP_value_weighted_index(df_msf)
    df_eq_idx = calc_equal_weighted_index(df_msf)
    df_msix = df_msix.rename(columns={"caldt": "date"})

    df = df_msix.merge(
        df_vw_idx.reset_index(),
        on="date",
        how="inner",
        suffixes=("", "_manual"),
    )
    df = df.merge(
        df_eq_idx.reset_index(),
        on="date",
        suffixes=("", "_manual"),
    )
    df = df.set_index("date")
    return df


def _demo():
    df_msf = load_CRSP_stock.load_CRSP_monthly_file(data_dir=DATA_DIR)
    df_msix = load_CRSP_stock.load_CRSP_index_files(data_dir=DATA_DIR)

    df_eq_idx = calc_equal_weighted_index(df_msf)
    df_vw_idx = calc_CRSP_value_weighted_index(df_msf)
    df_idxs = calc_CRSP_indices_merge(df_msf, df_msix)
    df_idxs.head()


if __name__ == "__main__":
    pass
