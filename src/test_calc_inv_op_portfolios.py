import pandas as pd
from pandas.testing import assert_frame_equal
from load_CRSP_Compustat import *
from calc_op_inv_portfolios import *

import config

DATA_DIR = config.DATA_DIR

fp_root = DATA_DIR / 'famafrench'

op_inv_5x5 = fp_root / '25_Portfolios_OP_INV_5x5.xlsx'
op_inv_5x5_daily = fp_root / '25_Portfolios_OP_INV_5x5_daily.xlsx'
op_inv_5x5_Woutdiv = fp_root / '25_Portfolios_OP_INV_5x5_Wout_div.xlsx'


def load_data_from_excel(file_path):
    # Load all sheets except for the first one (description)
    xls = pd.ExcelFile(file_path)
    sheets = xls.sheet_names[1:]  # Exclude the description sheet
    data = {sheet: xls.parse(sheet, index_col='Date') for sheet in sheets}
    return data


# Load your data from Excel
'''
This file was created by CMPT_ME_BEME_OP_INV_RETS using the 202312 CRSP database.
It contains value- and equal-weighted returns for portfolios formed on OP and INV.

The portfolios are constructed at the end of June. OP is operating profits (sales - cost
of goods sold - selling, general and administrative expenses - interest expense) divided by book
at the last fiscal year end of the prior calendar year. INV is the change
in total assets from the fiscal year ending in year t-2 to the fiscal year ending in t-1,
divided by t-2 total assets.
Annual returns are from January to December.

Missing data are indicated by -99.99 or -999.

Please be aware that some of the value-weight averages of operating profitability for
deciles 1 and 10 are extreme. These are driven by extraordinary values of OP for
individual firms. We have spot checked the accounting data that produce the
extraordinary values and all the numbers we examined accurately reflect the
data in the firms accounting statements.

The break points include utilities and include financials.

The  portfolios  include utilities and include financials.

         0:   Average Value Weighted Returns -- Monthly
         1:   Average Equal Weighted Returns -- Monthly
         2:   Average Value Weighted Returns -- Annual
         3:   Average Equal Weighted Returns -- Annual
         4:   Number of Firms in Portfolios
         5:   Average Market Cap
         6:   For portfolios formed in June of year t. 
              Value Weight Average of BE/ME Calculated for June of t to June of t+1 as:
              Sum[ME(Mth) * BE(Fiscal Year t-1) / ME(Dec t-1)] / Sum[ME(Mth)]
              Where Mth is a month from June of t to June of t+1 and BE(Fiscal Year t-1) is adjusted for net stock issuance to Dec t-1
         7:   For portfolios formed in June of year t. 
              Value Weight Average of BE_FYt-1/ME_June t Calculated for June of t to June of t+1 as:
              Sum[ME(Mth) * BE(Fiscal Year t-1) / ME(Jun t)] / Sum[ME(Mth)] 
              Where Mth is a month from June of t to June of t+1 
              and BE(Fiscal Year t-1) is adjusted for net stock issuance to Jun t 
         8:   For portfolios formed in June of year t. 
              Value Weight Average of OP Calculated as: 
              Sum[ME(Mth) * OP(fiscal year t-1) / BE(fiscal year t-1)] / Sum[ME(Mth)]
              Where Mth is a month from June of t to June of t+1 
         9:   For portfolios formed in June of year t. 
              Value Weight Average of investment (rate of growth of assets) Calculated as: 
              Sum[ME(Mth) * Log(ASSET(t-1) / ASSET(t-2) / Sum[ME(Mth)]
              Where Mth is a month from June of t to June of t+1   
         
IF DAILY
         0:   Average Value Weighted Returns -- Daily
         1:   Average Equal Weighted Returns -- Daily
         2:   Number of Firms in Portfolios
         3:   Average Firm Size
 
 

'''
dfs = load_data_from_excel(op_inv_5x5)
div_monthly_val_wt_ret = dfs['0']
div_monthly_eq_wt_ret = dfs['1']
div_ann_val_wt_ret = dfs['2']
div_ann_eq_wt_ret = dfs['3']
div_num_firms = dfs['4']
div_ave_mkt_cap = dfs['5']
div_val_wt_ave_beme = dfs['6']
div_val_wt_ave_beme_lagged = dfs['7']
div_val_wt_ave_op = dfs['8']
div_val_wt_ave_inv = dfs['9']

dfs2 = load_data_from_excel(op_inv_5x5_Woutdiv)
monthly_val_wt_ret = dfs2['0']
monthly_eq_wt_ret = dfs2['1']
ann_val_wt_ret = dfs2['2']
ann_eq_wt_ret = dfs2['3']
num_firms = dfs2['4']
ave_mkt_cap = dfs2['5']
val_wt_ave_beme = dfs2['6']
val_wt_ave_beme_lagged = dfs2['7']
val_wt_ave_op = dfs2['8']
val_wt_ave_inv = dfs2['9']

dfs3 = load_data_from_excel(op_inv_5x5_daily)
daily_val_wt_ret = dfs3['0']
daily_eq_wt_ret = dfs3['1']
daily_num_firms = dfs3['2']
daily_ave_firm_size = dfs3['3']


def test_output_shape():
    # columns should be 25 (5x5 sorts)
    # rows should be 12*64 (Jan 1960 - Dec 2023) = 768
     comp = load_compustat(data_dir=DATA_DIR)
     crsp = load_CRSP_stock(data_dir=DATA_DIR)
     ccm = load_CRSP_Comp_Link_Table(data_dir=DATA_DIR)

     crsp2 = calculate_market_equity(ccm)
     crsp3, crsp_jun = use_dec_market_equity(crsp2)
     ccm2, ccm_jun = merge_CRSP_and_Compustat(crsp_jun, comp, ccm, crsp3)
    
     ccm3 = name_ports(ccm2)
     vwret_m, ewret_m, num_firms = create_op_inv_portfolios(ccm3)

     assert num_firms.shape == (768, 25)
     
def test_5x5_file_exists():
        # Assuming DATADIR is an environment variable. Replace with your actual path if needed

        manual_dir = os.path.join(DATA_DIR, 'manual')
        target_file = os.path.join(manual_dir, '5x5_OP_INV_portfolios.xlsx')

        # Check if the file exists
        assert(os.path.isfile(target_file), f"{target_file} does not exist")