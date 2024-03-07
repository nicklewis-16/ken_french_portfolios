"""
This module provides functionality for assigning industry classifications to companies based on SIC codes, 
creating value-weighted industry portfolios, and visualizing the monthly number of securities by industry.

It utilizes datasets from CRSP (Center for Research in Security Prices) and Compustat to calculate market equity,
determine industry assignments, and generate industry portfolios that are further analyzed through various 
visualizations. The module makes extensive use of pandas for data manipulation, plotnine for plotting, and 
additional utilities from the pathlib, datetime, and custom modules for data handling and configuration.

Features include:
- Assigning companies to industries based on SIC codes with both broad (5 industries) and detailed (49 industries) classifications.
- Calculating market equity for companies using CRSP data.
- Creating value-weighted returns for industry portfolios.
- Visualizing the monthly count of securities per industry over time.
- Saving calculated industry portfolio returns and counts to Excel files for further analysis.

The module expects specific data structure in the input CRSP and Compustat datasets and relies on external
configuration for specifying input and output directories.

Functions:
- assign_industry5(sic_code): Assigns a broad industry category based on the SIC code.
- assign_industry49(sic_code): Assigns a detailed industry portfolio based on the SIC code.
- draw_industry_assignment(securities_per_industry, name, n): Draws a line plot showing the monthly number of securities by industry.
- wavg(group, avg_name, weight_name): Calculates value-weighted returns.
- calculate_market_equity(crsp): Calculates the market equity for observations in the CRSP dataset.
- use_dec_market_equity(crsp2): Utilizes December market equity to calculate market equity at different time points.
- create_industry_portfolios(ccm4, n): Creates value-weighted industry portfolios and counts firms in each.

This script is intended to be run as the main module, loading data from specified directories, performing 
calculations, and saving results to Excel files for both 5 and 49 industry classifications.

Dependencies:
- load_CRSP_Compustat, load_CRSP_stock for loading datasets
"""


import pandas as pd
import numpy as np
from pathlib import Path
import config
from datetime import datetime
from matplotlib.ticker import FuncFormatter
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

OUTPUT_DIR = Path(config.OUTPUT_DIR)
DATA_DIR = Path(config.DATA_DIR)

from load_CRSP_Compustat import *
from load_CRSP_stock import *


def assign_industry5(sic_code):
    """
    Assigns an industry category based on the given SIC code.

    Parameters:
    sic_code (int): The SIC code to be categorized.

    Returns:
    str: The industry category assigned based on the SIC code range.
    """

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
    
    return 'Other'

def assign_industry49(sic_code):
    """
    Assigns an industry portfolio based on the given SIC code, using a comprehensive
    mapping of SIC codes to 49 distinct industry portfolios as specified.

    Parameters:
    sic_code (int): The SIC code to be categorized.

    Returns:
    str: The industry portfolio name assigned based on the SIC code range.
    """
    try:
        sic_code = int(sic_code)
    except ValueError:
        return 'Invalid SIC Code'  # Return error message if sic_code is not an integer

    # Define SIC code ranges for each of the 49 portfolios
    portfolios = {
        'Agric': [(100, 199), (200, 299), (700, 799), (910, 919), (2048, 2048)],
        'Food': [(2000, 2009), (2010, 2019), (2020, 2029), (2030, 2039), (2040, 2046), (2050, 2059), (2060, 2063), (2070, 2079), (2090, 2092), (2095, 2095), (2098, 2099)],
        'Soda': [(2064, 2068), (2086, 2086), (2087, 2087), (2096, 2096), (2097, 2097)],
        'Beer': [(2080, 2080), (2082, 2082), (2083, 2083), (2084, 2084), (2085, 2085)],
        'Smoke': [(2100, 2199)],
        'Toys': [(920, 999), (3650, 3651), (3732, 3732), (3930, 3931), (3940, 3949)],
        'Fun': [(7800, 7829), (7830, 7833), (7840, 7841), (7900, 7900), (7910, 7911), (7920, 7929), (7930, 7933), (7940, 7949), (7980, 7980), (7990, 7999)],
        'Books': [(2700, 2709), (2710, 2719), (2720, 2729), (2730, 2739), (2740, 2749), (2770, 2771), (2780, 2789), (2790, 2799)],
        'Hshld': [(2047, 2047), (2391, 2392), (2510, 2519), (2590, 2599), (2840, 2844), (3160, 3161), (3170, 3172), (3190, 3199), (3229, 3229), (3260, 3269), (3230, 3231), (3630, 3639), (3750, 3751), (3800, 3800), (3860, 3861), (3870, 3873), (3910, 3915), (3960, 3962), (3991, 3991), (3995, 3995)],
        'Clths': [(2300, 2390), (3020, 3021), (3100, 3111), (3130, 3131), (3140, 3149), (3150, 3151), (3963, 3965)],
        'Hlth': [(8000, 8099)],
        'MedEq': [(3693, 3693), (3840, 3849), (3850, 3851)],
        'Drugs': [(2830, 2836)],
        'Chems': [(2800, 2829), (2850, 2859), (2860, 2869), (2870, 2879), (2890, 2899)],
        'Rubbr': [(3031, 3031), (3041, 3041), (3050, 3053), (3060, 3069), (3070, 3079), (3080, 3089), (3090, 3099)],
        'Txtls': [(2200, 2269), (2270, 2279), (2280, 2284), (2290, 2295), (2297, 2297), (2298, 2298), (2299, 2299), (2393, 2395), (2397, 2399)],
        'BldMt': [(800, 899), (2400, 2439), (2450, 2459), (2490, 2499), (2660, 2661), (2950, 2952), (3200, 3200), (3210, 3211), (3240, 3241), (3250, 3259), (3261, 3261), (3264, 3264), (3270, 3275), (3280, 3281), (3290, 3293), (3295, 3299), (3420, 3429), (3430, 3433), (3440, 3441), (3442, 3442), (3446, 3446), (3448, 3448), (3449, 3449), (3450, 3451), (3452, 3452), (3490, 3499), (3996, 3996)],
        'Cnstr': [(1500, 1511), (1520, 1529), (1530, 1539), (1540, 1549), (1600, 1699), (1700, 1799)],
        'Steel': [(3300, 3300), (3310, 3317), (3320, 3325), (3330, 3339), (3340, 3341), (3350, 3357), (3360, 3369), (3370, 3379), (3390, 3399)],
        'FabPr': [(3400, 3400), (3443, 3444), (3460, 3469), (3470, 3479)],
        'Mach': [(3510, 3519), (3520, 3529), (3530, 3536), (3538, 3538), (3540, 3549), (3550, 3559), (3560, 3569), (3580, 3589), (3590, 3599)],
        'ElcEq': [(3600, 3600), (3610, 3613), (3620, 3629), (3640, 3649), (3660, 3660), (3690, 3692), (3699, 3699)],
        'Autos': [(2296, 2296), (2396, 2396), (3010, 3011), (3537, 3537), (3647, 3647), (3694, 3694), (3700, 3700), (3710, 3716), (3792, 3792), (3790, 3791), (3799, 3799)],
        'Aero': [(3720, 3729)],
        'Ships': [(3730, 3731), (3740, 3743)],
        'Guns': [(3760, 3769), (3795, 3795), (3480, 3489)],
        'Gold': [(1040, 1049)],
        'Mines': [(1000, 1009), (1010, 1019), (1020, 1029), (1030, 1039), (1050, 1059), (1060, 1069), (1070, 1079), (1080, 1089), (1090, 1099), (1100, 1119), (1400, 1499)],
        'Coal': [(1200, 1299)],
        'Oil': [(1300, 1300), (1310, 1319), (1320, 1329), (1330, 1339), (1370, 1379), (1380, 1389), (2900, 2912), (2990, 2999)],
        'Util': [(4900, 4900), (4910, 4911), (4920, 4925), (4930, 4939), (4940, 4942)],
        'Telcm': [(4800, 4800), (4810, 4813), (4820, 4822), (4830, 4839), (4840, 4841), (4880, 4889), (4890, 4899)],
        'PerSv': [(7020, 7021), (7030, 7033), (7200, 7200), (7210, 7219), (7220, 7221), (7230, 7231), (7240, 7241), (7250, 7251), (7260, 7269), (7270, 7299), (7395, 7395), (7500, 7500), (7520, 7529), (7530, 7539), (7540, 7549), (7600, 7600), (7620, 7629), (7630, 7631), (7640, 7641), (7690, 7699), (8100, 8199), (8200, 8299), (8300, 8399), (8400, 8499), (8600, 8699), (8800, 8899), (7510, 7515)],
        'BusSv': [(2750, 2759), (3993, 3993), (7218, 7218), (7300, 7300), (7310, 7319), (7320, 7329), (7330, 7339), (7340, 7349), (7350, 7359), (7360, 7369), (7374, 7374), (7376, 7376), (7377, 7377), (7378, 7378), (7379, 7379), (7380, 7389), (7391, 7391), (7392, 7392), (7393, 7393), (7394, 7394), (7396, 7396), (7397, 7397), (7399, 7399), (7519, 7519), (8700, 8700), (8710, 8713), (8720, 8721), (8730, 8734), (8740, 8748), (8900, 8910), (8911, 8911), (8920, 8999), (4220, 4229)],
        'Hardw': [(3570, 3579), (3680, 3689), (3695, 3695)],
        'Softw': [(7370, 7372), (7375, 7375), (7373, 7373)],
        'Chips': [(3622, 3622), (3661, 3669), (3670, 3679), (3810, 3810), (3812, 3812)],
        'LabEq': [(3811, 3811), (3820, 3829), (3830, 3839)],
        'Paper': [(2520, 2549), (2600, 2639), (2670, 2699), (2760, 2761), (3950, 3955)],
        'Boxes': [(2440, 2449), (2640, 2659), (3220, 3221), (3410, 3412)],
        'Trans': [(4000, 4013), (4040, 4049), (4100, 4100), (4110, 4119), (4120, 4121), (4130, 4131), (4140, 4142), (4150, 4151), (4170, 4173), (4190, 4199), (4200, 4200), (4210, 4219), (4230, 4231), (4240, 4249), (4400, 4499), (4500, 4599), (4600, 4699), (4700, 4700), (4710, 4712), (4720, 4729), (4730, 4739), (4740, 4749), (4780, 4780), (4782, 4782), (4783, 4783), (4784, 4784), (4785, 4785), (4789, 4789)],
        'Whlsl': [(5000, 5000), (5010, 5015), (5020, 5023), (5030, 5039), (5040, 5049), (5050, 5059), (5060, 5065), (5070, 5088), (5090, 5099), (5100, 5100), (5110, 5113), (5120, 5122), (5130, 5139), (5140, 5149), (5150, 5159), (5160, 5169), (5170, 5172), (5180, 5182), (5190, 5199)],
        'Rtail': [(5200, 5200), (5210, 5219), (5220, 5229), (5230, 5231), (5250, 5251), (5260, 5261), (5270, 5271), (5300, 5300), (5310, 5311), (5320, 5320), (5330, 5331), (5334, 5334), (5340, 5349), (5390, 5399), (5400, 5400), (5410, 5412), (5420, 5429), (5430, 5439), (5440, 5449), (5450, 5459), (5460, 5469), (5490, 5499), (5500, 5500), (5510, 5599), (5600, 5699), (5700, 5700), (5710, 5719), (5720, 5722), (5730, 5736), (5750, 5799), (5900, 5900), (5910, 5912), (5920, 5929), (5930, 5932), (5940, 5949), (5950, 5959), (5960, 5969), (5970, 5979), (5980, 5989), (5990, 5999)],
        'Meals': [(5800, 5819), (5820, 5829), (5890, 5899), (7000, 7000), (7010, 7019), (7040, 7049), (7213, 7213)],
        'Banks': [(6000, 6000), (6010, 6019), (6020, 6029), (6030, 6036), (6040, 6059), (6060, 6062), (6080, 6082), (6090, 6099)],
        'Insur': [(6300, 6300), (6310, 6319), (6320, 6329), (6330, 6331), (6350, 6351), (6360, 6361), (6370, 6379), (6390, 6399), (6400, 6411)],
        'RlEst': [(6500, 6500), (6510, 6519), (6520, 6529), (6530, 6532), (6540, 6541), (6550, 6553), (6590, 6599), (6610, 6611)],
        'Fin': [(6200, 6299), (6700, 6700), (6710, 6719), (6720, 6726), (6730, 6733), (6740, 6779), (6790, 6799)],
        'Other': [(4950, 4959), (4960, 4961), (4970, 4971), (4990, 4991)]
    }

    # Iterate over the portfolios and their SIC code ranges to find a match
    for portfolio_name, ranges in portfolios.items():
        for start, end in ranges:
            if start <= sic_code <= end:
                return portfolio_name

    return 'Other'  # Default category if no ranges match

    
def wavg(group, avg_name, weight_name):
    """function to calculate value weighted return
    """
    d = group[avg_name]
    w = group[weight_name]
    try:
        return (d * w).sum() / w.sum()
    except ZeroDivisionError:
        return np.nan
    
def calculate_market_equity(crsp):
    """
    Calculate the market equity for each observation in the given CRSP dataset.

    Parameters:
    crsp (DataFrame): The CRSP dataset containing the necessary columns.

    Returns:
    DataFrame: The updated CRSP dataset with the market equity calculated.

    """
    crsp = crsp.sort_values(by=["date", "permco", "me"])

    ### Aggregate Market Cap ###
    # sum of me across different permno belonging to same permco a given date
    crsp_summe = crsp.groupby(["date", "permco"])["me"].sum().reset_index()

    # largest mktcap within a permco/date
    crsp_maxme = crsp.groupby(["date", "permco"])["me"].max().reset_index()

    # join by jdate/maxme to find the permno
    crsp1 = pd.merge(crsp, crsp_maxme, how="inner", on=["date", "permco", "me"])

    # drop me column and replace with the sum me
    crsp1 = crsp1.drop(["me"], axis=1)

    # join with sum of me to get the correct market cap info
    crsp2 = pd.merge(crsp1, crsp_summe, how="inner", on=["date", "permco"])

    # sort by permno and date and also drop duplicates
    crsp2 = crsp2.sort_values(by=["permno", "date"]).drop_duplicates()
    return crsp2


def use_dec_market_equity(crsp2):
    """
    Calculate market equity using December market cap and June market cap.

    Parameters:
    crsp2 (DataFrame): Input DataFrame containing market cap data.

    Returns:
    crsp3 (DataFrame): DataFrame with calculated market equity values.
    crsp_jun (DataFrame): DataFrame with market equity values as of June.

    Notes:
    - The function calculates market equity using December market cap and June market cap.
    - The December market cap is used to create the Book-to-Market ratio (BEME).
    - The June market cap must be positive in order to be included in the portfolio.
    - The function returns two DataFrames: crsp3 and crsp_jun.
    - crsp3 contains the calculated market equity values.
    - crsp_jun contains the market equity values as of June.

    Finally, ME at June and December
    were flagged since (1) December ME will be used to create Book-to-Market
    ratio (BEME) and (2) June ME has to be positive in order to be part of
    the portfolio.'

    """
    # keep December market cap
    crsp2["year"] = crsp2["date"].dt.year
    crsp2["month"] = crsp2["date"].dt.month
    decme = crsp2[crsp2["month"] == 12]
    decme = decme[["permno", "date", "me", "year"]].rename(
        columns={"me": "dec_me"}
    )

    ### July to June dates
    crsp2["ffdate"] = crsp2["date"] + MonthEnd(-6)
    crsp2["ffyear"] = crsp2["ffdate"].dt.year
    crsp2["ffmonth"] = crsp2["ffdate"].dt.month
    crsp2["1+retx"] = 1 + crsp2["retx"]
    crsp2 = crsp2.sort_values(by=["permno", "month"])

    # cumret by stock
    crsp2["cumretx"] = crsp2.groupby(["permno", "ffyear"])["1+retx"].cumprod()

    # lag cumret
    crsp2["L_cumretx"] = crsp2.groupby(["permno"])["cumretx"].shift(1)

    # lag market cap
    crsp2["L_me"] = crsp2.groupby(["permno"])["me"].shift(1)

    # if first permno then use me/(1+retx) to replace the missing value
    crsp2["count"] = crsp2.groupby(["permno"]).cumcount()
    crsp2["L_me"] = np.where(
        crsp2["count"] == 0, crsp2["me"] / crsp2["1+retx"], crsp2["L_me"]
    )

    # baseline me
    mebase = crsp2[crsp2["ffmonth"] == 1][["permno", "ffyear", "L_me"]].rename(
        columns={"L_me": "mebase"}
    )

    # merge result back together
    crsp3 = pd.merge(crsp2, mebase, how="left", on=["permno", "ffyear"])
    crsp3["wt"] = np.where(
        crsp3["ffmonth"] == 1, crsp3["L_me"], crsp3["mebase"] * crsp3["L_cumretx"]
    )

    decme["year"] = decme["year"] + 1
    decme = decme[["permno", "year", "dec_me"]]

    # Info as of June
    crsp3_jun = crsp3[crsp3["month"] == 6]

    crsp_jun = pd.merge(crsp3_jun, decme, how="inner", on=["permno", "year"])

    crsp_jun = crsp_jun.sort_values(by=["permno", "month"]).drop_duplicates()
    return crsp3, crsp_jun



def create_industry_portfolios(ccm4,n):
    """Create value-weighted industry portfolios
    and provide count of firms in each portfolio.
    """
    # value-weighted return
    vwret = (
        ccm4.groupby(["date", f"industry{n}"])
        .apply(wavg, "ret", "wt")
        .to_frame()
        .reset_index()
        .rename(columns={0: "vwret"})
    )
    
    # firm count
    vwret_n = (
        ccm4.groupby(["date", f"industry{n}"])["ret"]
        .count()
        .reset_index()
    )

    return vwret, vwret_n




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

filename = DATA_DIR / 'manual' / '5industry_portfolios.xlsx'

with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
    vwret5piv.to_excel(writer, sheet_name='VW Avg Mo. Ret', index=True)
    vwret_5npiv.to_excel(writer, sheet_name='Num Firms', index=True)
    
filename = DATA_DIR / 'manual' / '49industry_portfolios.xlsx'

with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
    vwret49piv.to_excel(writer, sheet_name='VW Avg Mo. Ret', index=True)
    vwret_49npiv.to_excel(writer, sheet_name='Num Firms', index=True)

