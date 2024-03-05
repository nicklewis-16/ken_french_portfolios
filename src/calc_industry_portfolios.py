import pandas as pd
import numpy as np
from pathlib import Path
import config


OUTPUT_DIR = Path(config.OUTPUT_DIR)
DATA_DIR = Path(config.DATA_DIR)

from load_CRSP_Compustat import *
from load_CRSP_stock import *



# def merge_datasets(ccm, comp, crsp):
#     """Merge the CCM link table with Compustat and CRSP data, focusing on required keys and columns."""
#     # Merge CCM with Compustat
#     ccm_comp = pd.merge(ccm, comp, how='left', on='gvkey')
#     # Merge the result with CRSP
#     merged_data = pd.merge(ccm_comp, crsp, how='left', on='permno', suffixes=('_comp', '_crsp'))
#     merged_data['sic_code'] = merged_data['sich'].where(merged_data['sich'].notnull(), merged_data['siccd'])
#     merged_data['industry5'] = merged_data['sic_code'].apply(assign_industry)
#     return merged_data


def assign_industry(sic_code):
    try:
        sic_code = int(sic_code)  # Ensure SIC code is an integer
    except ValueError:
        return 'Other'  # Return 'Other' if SIC code cannot be converted to integer

    # Define SIC code ranges for each industry
    cnsmr_ranges = [(100, 999), (2000, 2399), (2700, 2749), (2770, 2799), (3100, 3199),
                    (3940, 3989), (2500, 2519), (2590, 2599), (3630, 3659), (3710, 3711),
                    (3714, 3714), (3716, 3716), (3750, 3751), (3792, 3792), (3900, 3939),
                    (3990, 3999), (5000, 5999), (7200, 7299), (7600, 7699)]
    manuf_ranges = [(2520, 2589), (2600, 2699), (2750, 2769), (2800, 2829), (2840, 2899),
                    (3000, 3099), (3200, 3569), (3580, 3621), (3623, 3629), (3700, 3709),
                    (3712, 3713), (3715, 3715), (3717, 3749), (3752, 3791), (3793, 3799),
                    (3860, 3899), (1200, 1399), (2900, 2999), (4900, 4949)]
    hitec_ranges = [(3570, 3579), (3622, 3622), (3660, 3692), (3694, 3699), (3810, 3839),
                    (7370, 7379), (7391, 7391), (8730, 8734), (4800, 4899)]
    hlth_ranges = [(2830, 2839), (3693, 3693), (3840, 3859), (8000, 8099)]

    # Assign industry based on SIC code range
    for start, end in cnsmr_ranges:
        if start <= sic_code <= end:
            return 'Cnsmr'
    for start, end in manuf_ranges:
        if start <= sic_code <= end:
            return 'Manuf'
    for start, end in hitec_ranges:
        if start <= sic_code <= end:
            return 'HiTec'
    for start, end in hlth_ranges:
        if start <= sic_code <= end:
            return 'Hlth'
    
    return 'Other'  # Default category if no ranges match



if __name__ == "__main__":
    comp = load_compustat(data_dir=DATA_DIR)
    crsp = load_CRSP_stock(data_dir=DATA_DIR)
    ccm = load_CRSP_Comp_Link_Table(data_dir=DATA_DIR)
    ccm['industry5'] = ccm['siccd'].apply(assign_industry)
    