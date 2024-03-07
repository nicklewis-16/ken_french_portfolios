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
from calc_op_inv_portfolios import *

if __name__ == "__main__":
    comp = load_compustat(data_dir=DATA_DIR)
    crsp = load_CRSP_stock(data_dir=DATA_DIR)
    ccm = load_CRSP_Comp_Link_Table(data_dir=DATA_DIR)

    crsp2 = calculate_market_equity(ccm)
    crsp3, crsp_jun = use_dec_market_equity(crsp2)
    ccm2, ccm_jun = merge_CRSP_and_Compustat(crsp_jun, comp, ccm, crsp3)
    
    ccm3 = name_ports(ccm2)
    vwret_m, ewret_m, num_firms = create_op_inv_portfolios(ccm3)

    # pic1 = vwret_m[[('value_weighted_ret', 'OP1', 'INV1'), ('value_weighted_ret', 'OP5', 'INV5')]].plot()
    # pic1.set_title("Monthly Weighted Returns for Mixed Portfolios")
    # pic1.legend(['OP1 INV1', 'OP5 INV5'])
    # pic1.savefig(OUTPUT_DIR / f"opinv_same.png", dpi=300)
    
    # pic2 = vwret_m[[('value_weighted_ret', 'OP1', 'INV5'), ('value_weighted_ret', 'OP5', 'INV1')]].plot()
    # pic2.set_title("Monthly Weighted Returns for Mixed Portfolios")
    # pic2.legend(['OP1 INV5', 'OP5 INV1'])
    # pic2.savefig(OUTPUT_DIR / f"opinv_mixed.png", dpi=300)



    fig, ax = plt.subplots(figsize=(12, 12))
    ax.plot(vwret_m[[('value_weighted_ret', 'OP1', 'INV5'), ('value_weighted_ret', 'OP5', 'INV1')]])
    ax.set_title("Monthly Weighted Returns for Mixed Portfolios")
    
    # Rotate and align the tick labels so they look better.
    fig.autofmt_xdate()

    # Add a legend.
    ax.legend(['OP1 INV5', 'OP5 INV1'])

    # Save the figure to a file in the output directory.
    fig.savefig(OUTPUT_DIR / f"opinv_mixed.png", dpi=300)
    plt.close(fig)  




    fig, ax = plt.subplots(figsize=(12, 12))
    ax.plot(vwret_m[[('value_weighted_ret', 'OP1', 'INV1'), ('value_weighted_ret', 'OP5', 'INV5')]])
    ax.set_title("Monthly Weighted Returns for Homogenous Portfolios")
    
    # Rotate and align the tick labels so they look better.
    fig.autofmt_xdate()

    # Add a legend.
    ax.legend(['OP1 INV1', 'OP5 INV5'])

    # Save the figure to a file in the output directory.
    fig.savefig(OUTPUT_DIR / f"opinv_same.png", dpi=300)
    plt.close(fig)  