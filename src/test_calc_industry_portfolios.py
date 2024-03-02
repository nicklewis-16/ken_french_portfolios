import pandas as pd
from pandas.testing import assert_frame_equal
from calc_inv_op_portfolios import *
from load_CRSP_Compustat import *

import config

DATA_DIR = config.DATA_DIR

fp_root = DATA_DIR / 'famafrench'

ind5_port = fp_root / '5_Industry_Portfolios.xlsx'
ind5_daily = fp_root / '5_Industry_Portfolios_daily.xlsx'
ind5_port_Woutdiv = fp_root / '5_Industry_Portfolios_Wout_Div.xlsx'


def load_data_from_excel(file_path):
    # Load all sheets except for the first one (description)
    xls = pd.ExcelFile(file_path)
    sheets = xls.sheet_names[1:]  # Exclude the description sheet
    data = {sheet: xls.parse(sheet, index_col='Date') for sheet in sheets}
    return data


# Load your data from Excel
'''
5 Industry Portfolios
This file was created by CMPT_IND_RETS using the 202401 CRSP database. 
It contains value- and equal-weighted returns for 5 industry portfolios.
The portfolios are constructed at the end of June. 
The annual returns are from January to December. Missing data are indicated by -99.99 or -999. 
Copyright 2024 Kenneth R. French
0 : Average Value Weighted Returns -- Monthly (1170 rows x 5 cols)
1 : Average Equal Weighted Returns -- Monthly (1170 rows x 5 cols)
2 : Average Value Weighted Returns -- Annual (97 rows x 5 cols) 
3 : Average Equal Weighted Returns -- Annual (97 rows x 5 cols)
4 : Number of Firms in Portfolios (1170 rows x 5 cols)
5 : Average Firm Size (1170 rows x 5 cols)
6 : Sum of BE / Sum of ME (98 rows x 5 cols)
7 : Value-Weighted Average of BE/ME (98 rows x 5 cols)
'''
dfs = load_data_from_excel(ind5_port)
div_monthly_val_wt_ret = dfs['0']
div_monthly_eq_wt_ret = dfs['1']
div_ann_val_wt_ret = dfs['2']
div_ann_eq_wt_ret = dfs['3']
div_num_firms = dfs['4']
div_ave_frm_sz = dfs['5']
div_beme = dfs['6']
div_val_wt_ave_beme = dfs['7']

dfs2 = load_data_from_excel(ind5_port_Woutdiv)
monthly_val_wt_ret = dfs2['0']
monthly_eq_wt_ret = dfs2['1']
ann_val_wt_ret = dfs2['2']
ann_eq_wt_ret = dfs2['3']
num_firms = dfs2['4']
ave_frm_sz = dfs2['5']
beme = dfs2['6']
val_wt_ave_beme = dfs['7']

dfs3 = load_data_from_excel(ind5_daily)
daily_val_wt_ret = dfs3['0']
daily_eq_wt_ret = dfs3['1']


comp = load_compustat(data_dir=DATA_DIR)
crsp = load_CRSP_stock_ciz(data_dir=DATA_DIR)
ccm = load_CRSP_Comp_Link_Table(data_dir=DATA_DIR)
