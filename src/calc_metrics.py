import pandas as pd
import numpy as np
from pathlib import Path
import config
import pull_raw_data

OUTPUT_DIR = Path(config.OUTPUT_DIR)
DATA_DIR = Path(config.DATA_DIR)

def get_december_price(df):
    """
    Approximate the December price for each permno by getting the latest available price of the year.
    """
    df['year'] = df['date'].dt.year
    december_prices = df.sort_values('date').groupby(['year', 'permno']).tail(1)
    return december_prices[['permno', 'year', 'mthprc']].rename(columns={'mthprc': 'dec_price'})

def calculate_market_equity(df, december_prices):
    """
    Calculate market equity using the approximated December price.
    """
    df = df.merge(december_prices, on=['permno', 'year'], how='left')
    df['me'] = df['dec_price'].abs() * df['shrout']
    return df

def calculate_ep_ratio(df, december_prices):
    """
    Calculate E/P ratio using earnings per share (epsfx) and the approximated December price.
    """
    df = df.dropna(subset=['epsfx'])
    
    if 'dec_price' not in df.columns:
        df = df.merge(december_prices[['permno', 'dec_price']], on='permno', how='left')
    
    df['e_p'] = df['epsfx'] / df['dec_price'].abs()
    
    return df

def calculate_breakpoints(df):
    df_nyse = df[(df['exchange_name'] == 'NYSE') & (df['jdate'].dt.month == 6)]
    
    median_me = df_nyse.groupby(df_nyse['jdate'].dt.year)['me'].median()
    
    ep_30 = df_nyse.groupby(df_nyse['jdate'].dt.year)['e_p'].quantile(0.3)
    ep_70 = df_nyse.groupby(df_nyse['jdate'].dt.year)['e_p'].quantile(0.7)
    
    return median_me, ep_30, ep_70

def sort_into_portfolios(df, median_me, ep_30, ep_70):
    df['year'] = df['jdate'].dt.year
    df['size_category'] = df.apply(lambda row: 'Small' if row['me'] < median_me.get(row['year'], np.nan) else 'Big', axis=1)
    df['e_p_category'] = df.apply(
        lambda row: 'Low E/P' if row['e_p'] < ep_30.get(row['year'], np.nan) else (
            'High E/P' if row['e_p'] > ep_70.get(row['year'], np.nan) else 'Mid E/P'),
        axis=1
    )
    return df

def calculate_portfolio_returns(df):
    df['period'] = df['date'].dt.to_period('M')
    df['total_me'] = df.groupby(['period', 'size_category', 'e_p_category'])['me'].transform('sum')
    df['weight'] = df['me'] / df['total_me']
    df['value_weighted_return'] = df['mthret'] * df['weight']
    portfolio_returns = df.groupby(['period', 'size_category', 'e_p_category'])['value_weighted_return'].sum()
    portfolio_returns = portfolio_returns.unstack(level=['size_category', 'e_p_category'])
    portfolio_returns.index = portfolio_returns.index.strftime('%Y%m')
    portfolio_returns.columns = ['SMALL LoEP', 'SMALL HiEP', 'BIG LoEP', 'BIG HiEP', 'ME1 EP2', 'ME2 EP2']
    portfolio_returns = portfolio_returns[['SMALL LoEP', 'ME1 EP2', 'SMALL HiEP', 'BIG LoEP', 'ME2 EP2', 'BIG HiEP']]
    portfolio_returns.reset_index(inplace=True)
    portfolio_returns.rename(columns={'index': 'period'}, inplace=True)
    cols = ['period'] + [col for col in portfolio_returns.columns if col != 'period']
    portfolio_returns = portfolio_returns[cols]

    return portfolio_returns

def demo():
    df = pd.read_parquet(DATA_DIR / "pulled" / "merged_CRSP_Compustat_data.parquet")
    df['date'] = pd.to_datetime(df['date'])
    df['year'] = df['date'].dt.year
    december_prices = get_december_price(df)
    df = calculate_market_equity(df, december_prices)
    df = calculate_ep_ratio(df, december_prices)
    median_me, ep_30, ep_70 = calculate_breakpoints(df)
    df = sort_into_portfolios(df, median_me, ep_30, ep_70)
    portfolio_returns = calculate_portfolio_returns(df)

    portfolio_returns.to_csv(OUTPUT_DIR / 'portfolio_returns.csv', index=False)

if __name__ == "__main__":
    demo()




