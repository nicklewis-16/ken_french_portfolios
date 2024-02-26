import pandas as pd
from pandas.testing import assert_frame_equal
from calc_metrics import *

import config

DATA_DIR = config.DATA_DIR

fp_root = DATA_DIR / 'famafrench'

me_cfp_2x3_wout_div = DATA_DIR / '6_Portfolios_ME_CFP_2x3_Wout_Div.xlsx'
me_cfp_2x3 = DATA_DIR / '6_Portfolios_ME_CFP_2x3.xlsx'
me_dp_2x3_wout_div = DATA_DIR / '6_Portfolios_ME_DP_2x3_Wout_Div.xlsx'
me_dp_2x3 = DATA_DIR / '6_Portfolios_ME_DP_2x3.xlsx'
me_ep_2x3_wout_div = DATA_DIR / '6_Portfolios_ME_EP_2x3_Wout_Div.xlsx'
me_ep_2x3 = DATA_DIR / '6_Portfolios_ME_EP_2x3.xlsx'

def load_data_from_excel(file_path):
    # Load all sheets except for the first one (description)
    xls = pd.ExcelFile(file_path)
    sheets = xls.sheet_names[1:]  # Exclude the description sheet
    data = {sheet: xls.parse(sheet, index_col='Date') for sheet in sheets}
    return data


dfs1 = load_data_from_excel(me_cfp_2x3_wout_div)
dfs2 = load_data_from_excel(me_cfp_2x3)
dfs3 = load_data_from_excel(me_dp_2x3_wout_div)
dfs4 = load_data_from_excel(me_dp_2x3)
dfs5 = load_data_from_excel(me_ep_2x3_wout_div)
dfs6 = load_data_from_excel(me_ep_2x3)

# For ME_CFP_2x3_Wout_Div
import pandas as pd
from pandas.testing import assert_frame_equal
from calc_metrics import *

import config

DATA_DIR = config.DATA_DIR

fp_root = DATA_DIR / 'famafrench'

me_cfp_2x3_wout_div = DATA_DIR / '6_Portfolios_ME_CFP_2x3_Wout_Div.xlsx'
me_cfp_2x3 = DATA_DIR / '6_Portfolios_ME_CFP_2x3.xlsx'
me_dp_2x3_wout_div = DATA_DIR / '6_Portfolios_ME_DP_2x3_Wout_Div.xlsx'
me_dp_2x3 = DATA_DIR / '6_Portfolios_ME_DP_2x3.xlsx'
me_ep_2x3_wout_div = DATA_DIR / '6_Portfolios_ME_EP_2x3_Wout_Div.xlsx'
me_ep_2x3 = DATA_DIR / '6_Portfolios_ME_EP_2x3.xlsx'

def load_data_from_excel(file_path):
    # Load all sheets except for the first one (description)
    xls = pd.ExcelFile(file_path)
    sheets = xls.sheet_names[1:]  # Exclude the description sheet
    data = {sheet: xls.parse(sheet, index_col='Date') for sheet in sheets}
    return data


dfs1 = load_data_from_excel(me_cfp_2x3_wout_div)
dfs2 = load_data_from_excel(me_cfp_2x3)
dfs3 = load_data_from_excel(me_dp_2x3_wout_div)
dfs4 = load_data_from_excel(me_dp_2x3)
dfs5 = load_data_from_excel(me_ep_2x3_wout_div)
dfs6 = load_data_from_excel(me_ep_2x3)

# For ME_CFP_2x3_Wout_Div
monthly_val_wt_ret_cfp_wout_div = dfs1['0']
monthly_eq_wt_ret_cfp_wout_div = dfs1['1']
ann_val_wt_ret_cfp_wout_div = dfs1['2']
ann_eq_wt_ret_cfp_wout_div = dfs1['3']
num_firms_cfp_wout_div = dfs1['4']
ave_mkt_cap_cfp_wout_div = dfs1['5']
val_wt_ave_cfp_cfp_wout_div = dfs1['6']

# For ME_CFP_2x3
monthly_val_wt_ret_cfp = dfs2['0']
monthly_eq_wt_ret_cfp = dfs2['1']
ann_val_wt_ret_cfp = dfs2['2']
ann_eq_wt_ret_cfp = dfs2['3']
num_firms_cfp = dfs2['4']
ave_mkt_cap_cfp = dfs2['5']
val_wt_ave_cfp = dfs2['6']

# For ME_DP_2x3_Wout_Div
monthly_val_wt_ret_dp_wout_div = dfs3['0']
monthly_eq_wt_ret_dp_wout_div = dfs3['1']
ann_val_wt_ret_dp_wout_div = dfs3['2']
ann_eq_wt_ret_dp_wout_div = dfs3['3']
num_firms_dp_wout_div = dfs3['4']
ave_mkt_cap_dp_wout_div = dfs3['5']
val_wt_ave_dp_dp_wout_div = dfs3['6']

# For ME_DP_2x3
monthly_val_wt_ret_dp = dfs4['0']
monthly_eq_wt_ret_dp = dfs4['1']
ann_val_wt_ret_dp = dfs4['2']
ann_eq_wt_ret_dp = dfs4['3']
num_firms_dp = dfs4['4']
ave_mkt_cap_dp = dfs4['5']
val_wt_ave_dp = dfs4['6']

# For ME_EP_2x3_Wout_Div
monthly_val_wt_ret_ep_wout_div = dfs5['0']
monthly_eq_wt_ret_ep_wout_div = dfs5['1']
ann_val_wt_ret_ep_wout_div = dfs5['2']
ann_eq_wt_ret_ep_wout_div = dfs5['3']
num_firms_ep_wout_div = dfs5['4']
ave_mkt_cap_ep_wout_div = dfs5['5']
val_wt_ave_ep_ep_wout_div = dfs5['6']

# For ME_EP_2x3
monthly_val_wt_ret_ep = dfs6['0']
monthly_eq_wt_ret_ep = dfs6['1']
ann_val_wt_ret_ep = dfs6['2']
ann_eq_wt_ret_ep = dfs6['3']
num_firms_ep = dfs6['4']
ave_mkt_cap_ep = dfs6['5']
val_wt_ave_ep = dfs6['6']

def test_num_firms():
    """
    Test the 'Number of Firms' for all 6 portfolios.
    """
    for i, dfs in enumerate([dfs1, dfs2, dfs3, dfs4, dfs5, dfs6], start=1):
        expected = dfs['4'].describe().to_string()
        print(f"Portfolio {i} Number of Firms Summary:\n{expected}\n")
    pass

def test_value_weighted_returns():
    """
    Test the value-weighted returns for the last 3 years (36 months) across all 6 portfolios.
    """
    for i, dfs in enumerate([dfs1, dfs2, dfs3, dfs4, dfs5, dfs6], start=1):
        expected = dfs['0'].tail(36)  # Last 3 years (36 months)
        print(f"Portfolio {i} Value Weighted Returns (last 3 years):\n{expected}\n")
    pass #could add tolerance parameter

def test_equal_weighted_returns():
    """
    Test the equal-weighted returns for the last 3 years (36 months) across all 6 portfolios.
    """
    for i, dfs in enumerate([dfs1, dfs2, dfs3, dfs4, dfs5, dfs6], start=1):
        expected = dfs['1'].tail(36)  # Last 3 years (36 months)
        print(f"Portfolio {i} Equal Weighted Returns (last 3 years):\n{expected}\n")
    pass #could add tolerance parameter


## Other testings ideas 

def test_market_equity_calculation():
    """
    Verify that market equity (ME) is accurately calculated for each firm across all portfolios. 
    """
    pass

def test_breakpoint_calculation():
    """
    Assess the calculation of breakpoints for ME and the CF/P, D/P, E/P ratios.
    """
    pass


def test_ep_avg():
    """
    Test the 'Value Weight Average of E/P when portfolio is formed' for all 6 portfolios.
    """
    for i, dfs in enumerate([dfs1, dfs2, dfs3, dfs4, dfs5, dfs6], start=1):
        expected = dfs['6'].describe().to_string()
        print(f"Portfolio {i} Value Weight Average of E/P Summary:\n{expected}\n")
    pass

def test_avg_firm_size():
    """
    Test the 'Average Firm Size' for all 6 portfolios.
    """
    for i, dfs in enumerate([dfs1, dfs2, dfs3, dfs4, dfs5, dfs6], start=1):
        expected = dfs['5'].describe().to_string()
        print(f"Portfolio {i} Average Firm Size Summary:\n{expected}\n")
    pass
