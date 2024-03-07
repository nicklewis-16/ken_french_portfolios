import pandas as pd
from pandas.testing import assert_frame_equal
from load_CRSP_Compustat import *
from calc_industry_portfolios import *

import config

DATA_DIR = config.DATA_DIR

fp_root = DATA_DIR / 'famafrench'

ind5_port = fp_root / '5_Industry_Portfolios.xlsx'
ind5_daily = fp_root / '5_Industry_Portfolios_daily.xlsx'
ind5_port_Woutdiv = fp_root / '5_Industry_Portfolios_Wout_Div.xlsx'

ind49_port = fp_root / '49_Industry_Portfolios.xlsx'
ind49_daily = fp_root / '49_Industry_Portfolios_daily.xlsx'
ind49_port_Woutdiv = fp_root / '49_Industry_Portfolios_Wout_Div.xlsx'

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
div5_monthly_val_wt_ret = dfs['0']
div5_monthly_eq_wt_ret = dfs['1']
div5_ann_val_wt_ret = dfs['2']
div5_ann_eq_wt_ret = dfs['3']
div5_num_firms = dfs['4']
div5_ave_frm_sz = dfs['5']
div5_beme = dfs['6']
div5_val_wt_ave_beme = dfs['7']

dfs2 = load_data_from_excel(ind5_port_Woutdiv)
monthly5_val_wt_ret = dfs2['0']
monthly5_eq_wt_ret = dfs2['1']
ann5_val_wt_ret = dfs2['2']
ann5_eq_wt_ret = dfs2['3']
num5_firms = dfs2['4']
ave5_frm_sz = dfs2['5']
beme5 = dfs2['6']
val5_wt_ave_beme = dfs2['7']

dfs3 = load_data_from_excel(ind5_daily)
daily5_val_wt_ret = dfs3['0']
daily5_eq_wt_ret = dfs3['1']


dfs4 = load_data_from_excel(ind49_port)
div49_monthly_val_wt_ret = dfs4['0']
div49_monthly_eq_wt_ret = dfs4['1']
div49_ann_val_wt_ret = dfs4['2']
div49_ann_eq_wt_ret = dfs4['3']
div49_num_firms = dfs4['4']
div49_ave_frm_sz = dfs4['5']
div49_beme = dfs4['6']
div49_val_wt_ave_beme = dfs4['7']

dfs5 = load_data_from_excel(ind49_port_Woutdiv)
monthly49_val_wt_ret = dfs5['0']
monthly49_eq_wt_ret = dfs5['1']
ann49_val_wt_ret = dfs5['2']
ann49_eq_wt_ret = dfs5['3']
num49_firms = dfs5['4']
ave49_frm_sz = dfs5['5']
beme49 = dfs5['6']
val49_wt_ave_beme = dfs5['7']

dfs6 = load_data_from_excel(ind49_daily)
daily49_val_wt_ret = dfs6['0']
daily49_eq_wt_ret = dfs6['1']


comp = load_compustat(data_dir=DATA_DIR)
crsp = load_CRSP_stock(data_dir=DATA_DIR)
ccm = load_CRSP_Comp_Link_Table(data_dir=DATA_DIR)


crsp2 = calculate_market_equity(ccm)
crsp3, crsp_jun = use_dec_market_equity(crsp2)
crsp3['industry5'] = crsp3['siccd'].apply(assign_industry5)
crsp3['industry49'] = crsp3['siccd'].apply(assign_industry49)
vwret5, vwret_5n = create_industry_portfolios(crsp3, 5)
vwret49, vwret_49n = create_industry_portfolios(crsp3, 49)

vwret5piv = vwret5.pivot(index="date", columns="industry5", values="vwret") 
vwret49piv = vwret49.pivot(index="date", columns="industry49", values="vwret")
vwret_5npiv = vwret_5n.pivot(index="date", columns="industry5", values="ret")
vwret_49npiv = vwret_49n.pivot(index="date", columns="industry49", values="ret")

def test_5ind_file_exists():
        # Assuming DATADIR is an environment variable. Replace with your actual path if needed

        manual_dir = os.path.join(DATA_DIR, 'manual')
        target_file = os.path.join(manual_dir, '5industry_portfolios.xlsx')

        # Check if the file exists
        assert(os.path.isfile(target_file), f"{target_file} does not exist")

def test_49ind_file_exists():
        # Assuming DATADIR is an environment variable. Replace with your actual path if needed

        manual_dir = os.path.join(DATA_DIR, 'manual')
        target_file = os.path.join(manual_dir, '49industry_portfolios.xlsx')

        # Check if the file exists
        assert(os.path.isfile(target_file), f"{target_file} does not exist")


def test_output_shape():
    # columns should be 5 and 49
    # rows should be 12*64 (Jan 1960 - Dec 2023) = 768
    assert vwret_49npiv.shape == (768, 49)
    assert vwret_5npiv.shape == (768, 5)