import pandas as pd
from pandas.testing import assert_frame_equal
from calc_inv_op_portfolios import *

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


def test_num_firms():
    expected = """
            LoOP LoINV    OP1 INV2    OP1 INV3    OP1 INV4   LoOP HiINV    OP2 INV1    OP2 INV2   OP2 INV3    OP2 INV4    OP2 INV5    OP3 INV1    OP3 INV2    OP3 INV3    OP3 INV4    OP3 INV5    OP4 INV1    OP4 INV2    OP4 INV3    OP4 INV4    OP4 INV5  HiOP LoINV    OP5 INV2    OP5 INV3    OP5 INV4  HiOP HiINV
    count   726.000000  726.000000  726.000000  726.000000   726.000000  726.000000  726.000000  726.00000  726.000000  726.000000  726.000000  726.000000  726.000000  726.000000  726.000000  726.000000  726.000000  726.000000  726.000000  726.000000  726.000000  726.000000  726.000000  726.000000  726.000000
    mean    501.820937  181.292011  133.243802  135.590909   354.316804  105.871901  122.747934  127.53719  131.524793  154.688705   73.110193  104.764463  122.910468  135.585399  145.641873   61.286501   84.732782  103.323691  127.488981  158.297521   78.904959   72.006887   81.012397  112.812672  208.443526
    std     209.988337   75.412906   64.208452   71.952757   233.418461   32.264448   33.757285   45.73004   53.413405   70.163195   21.876052   28.265313   37.319001   50.325618   56.220881   19.406732   20.465755   29.811938   39.877838   61.610162   24.243206   21.797405   22.635428   33.848965  103.064862
    min     128.000000   38.000000   32.000000   25.000000    46.000000   36.000000   40.000000   40.00000   30.000000   31.000000   26.000000   42.000000   44.000000   39.000000   38.000000    9.000000   36.000000   40.000000   42.000000   46.000000   24.000000   20.000000   26.000000   60.000000   77.000000
    25%     364.500000  132.250000   94.000000   95.000000   145.000000   83.000000  107.000000  105.00000   98.000000  103.000000   56.000000   84.000000  107.000000  111.000000  114.000000   48.000000   72.000000   84.000000  104.000000  100.000000   62.000000   63.000000   68.000000   83.000000  116.000000
    50%     481.500000  186.000000  116.000000  122.000000   355.000000  107.000000  129.000000  127.00000  133.000000  157.500000   73.000000  112.000000  127.000000  134.500000  143.000000   59.500000   87.000000  112.000000  134.000000  170.000000   80.000000   76.000000   87.000000  107.000000  165.000000
    75%     648.000000  235.000000  167.000000  171.750000   454.750000  131.000000  144.000000  151.75000  165.000000  192.000000   84.000000  127.000000  141.000000  164.000000  179.000000   77.000000   98.000000  125.000000  152.000000  203.000000   96.000000   83.000000   96.000000  140.000000  309.000000
    max    1054.000000  375.000000  324.000000  369.000000  1101.000000  173.000000  190.000000  244.00000  267.000000  365.000000  132.000000  168.000000  209.000000  249.000000  277.000000  105.000000  128.000000  171.000000  220.000000  301.000000  141.000000  120.000000  132.000000  192.000000  421.000000
    """
    pass

def test_daily_average_firm_size():
    expected = """
            LoOP LoINV      OP1 INV2      OP1 INV3      OP1 INV4    LoOP HiINV      OP2 INV1      OP2 INV2      OP2 INV3      OP2 INV4      OP2 INV5      OP3 INV1      OP3 INV2      OP3 INV3      OP3 INV4      OP3 INV5      OP4 INV1      OP4 INV2      OP4 INV3      OP4 INV4      OP4 INV5    HiOP LoINV     OP5 INV2      OP5 INV3      OP5 INV4    HiOP HiINV
    count  15229.000000  15229.000000  15229.000000  15229.000000  15229.000000  15229.000000  15229.000000  15229.000000  15229.000000  15229.000000  15229.000000  15229.000000  15229.000000  15229.000000  15229.000000  15229.000000  15229.000000  15229.000000  15229.000000  15229.000000  15229.000000  15229.00000  15229.000000  15229.000000  15229.000000
    mean     354.155528    813.111199    962.025539    821.575985    727.820517   1719.814566   2295.838212   1993.254898   1971.299804   1898.520037   2308.298811   3195.808008   3075.517091   2751.505021   2717.267417   3188.280104   5205.977138   6564.693349   6150.662915   4339.434870   4973.957794   7765.15802   9250.784035   8616.208533   5195.762105
    std      430.619418   1017.316430   1174.534157   1129.787008    957.882768   2287.805478   3231.036942   2453.223878   2425.900127   2421.584578   3129.149185   4168.012631   3908.386486   3451.778339   4119.974046   4274.992632   6361.414681   8802.915629   8963.357968   6373.970206  10778.698925  11612.36860  14854.902455  12856.169166   7888.666925
    min       17.820000     45.430000     68.520000     29.080000     23.000000     51.730000    132.790000    199.610000     73.990000     52.900000     60.430000    124.700000    171.200000     97.780000     69.790000     44.430000     89.550000    208.330000    141.130000    117.840000     39.500000    168.39000    240.210000    237.840000     71.950000
    25%       58.570000    166.250000    202.920000    189.810000     88.420000    187.720000    449.050000    439.380000    444.430000    225.690000    217.850000    448.360000    708.550000    448.930000    234.480000    217.160000    461.940000    568.620000    429.800000    283.100000    218.100000    683.65000    709.130000    891.880000    330.560000
    50%      112.490000    279.590000    368.380000    418.620000    187.550000    464.500000    912.440000    729.360000    788.650000    461.820000   1114.880000   1695.640000   1279.900000   1044.630000    605.510000   1176.610000   2040.950000   2109.140000   1952.570000   1124.270000   1267.150000   2391.70000   2834.160000   2953.350000   1469.760000
    75%      601.570000   1287.340000   1684.050000   1015.800000   1193.470000   2530.630000   2294.710000   3034.380000   2641.920000   2974.360000   3280.540000   3634.420000   3654.880000   4075.050000   3571.800000   3436.330000   8558.510000   8546.300000   9260.530000   5950.890000   4414.880000   9968.99000  11913.170000  11857.620000   5703.440000
    max     2949.500000   7164.990000   7201.870000   7389.790000   5795.480000   9372.420000  15624.030000  11643.870000  13406.610000  11896.470000  15278.880000  20807.500000  24005.890000  21939.070000  22689.860000  17174.930000  31744.760000  49331.230000  48922.200000  34298.450000  68916.700000  81435.22000  83797.980000  72415.970000  39470.680000
    """
    pass
    

def test_calculate_op():
    # Assuming you have a predefined dataframe `df` you want to test against
    pass

def test_calculate_inv():
    # Similar setup for testing OP calculation
    pass

def test_form_portfolios():
    # Assuming `data` is loaded and contains the necessary columns for forming portfolios
    # You'll need to adapt this based on your actual data structure and what `form_portfolios` returns
    # portfolios = form_portfolios(data)
    # Assert something about the portfolios, such as the number of portfolios or their structure
    pass

def test_calculate_portfolio_returns():
    # This test would depend on the output format of `calculate_portfolio_returns`
    # For example, if it returns a DataFrame, you could use `assert_frame_equal` to compare it to an expected DataFrame
    pass



#### BRYCE TESTING IDEAS####

# def test_calc_book_equity_and_years_in_compustat():
#     """
#     ensures book equity is calculated correctly
#     """

# def test_subset_CRSP():
#     """
#     make sure each of the subsets results in the correct dataframe size
#     """

# def test_profitability():
#     """
#     ensure that the profitability is calculated correctly
#     """

# def test_investment():
#     """
#     ensure that the investment is calculated correctly
#     """

# def test_equal_weighted_index():
#     """
#     compare our calculated equal weighted index to the table we have downloaded off the internet (with a margin of error)
#     """
#     ## MAY WANT TO DO THIS WITH MORE PORTFOLIOS