'''

- draw_industry_assignment(securities_per_industry, name, n): Draws a line plot showing the monthly number of securities by industry.

'''

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
from calc_industry_portfolios import *
from test_calc_industry_portfolios import *


def draw_industry_assignment(securities_per_industry, name, n):
    """
    Draw a line plot showing the monthly number of securities by industry using matplotlib.

    Parameters:
    securities_per_industry (DataFrame): A DataFrame containing the data for securities per industry.

    Returns:
    None (plots the line plot and saves it to a file in output directory)
    """

    # Ensure 'date' is a datetime type for proper plotting.
    securities_per_industry['date'] = pd.to_datetime(securities_per_industry['date'])
    
    # Sort the DataFrame to ensure that the data is plotted in chronological order.
    securities_per_industry.sort_values('date', inplace=True)

    # Creating a figure and axis for plotting.
    fig, ax = plt.subplots(figsize=(10, 6))

    # Get unique industry names for coloring and linestyles.
    industries = securities_per_industry[name].unique()
    linetypes = ["-", "--", "-.", ":"]
    color_map = plt.cm.get_cmap('tab10', len(industries))  # Get a color map from matplotlib.

    for i, industry in enumerate(industries):
        # Filter the DataFrame for each industry.
        industry_data = securities_per_industry[securities_per_industry[name] == industry]
        
        # Plot each industry with a unique color and linestyle.
        ax.plot(industry_data['date'], industry_data['ret'],
                linestyle=linetypes[i % len(linetypes)], color=color_map(i),
                label=industry)

    # Set the title of the plot.
    ax.set_title("Monthly number of securities by industry")

    # Improve formatting of the x-axis to handle dates nicely.
    ax.xaxis.set_major_locator(mdates.YearLocator(10))  # Major ticks every 10 years.
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))  # Format as just the year.
    
    # Rotate and align the tick labels so they look better.
    fig.autofmt_xdate()

    # Use a formatter for the y-axis to ensure commas are used for thousands.
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: '{:,.0f}'.format(x))) 

    # Add a legend.
    ax.legend()

    # Save the figure to a file in the output directory.
    fig.savefig(OUTPUT_DIR / f"sec_per_ind_{n}.png", dpi=300)
    plt.close(fig)  
    
    
    
if __name__ == "__main__":
    
    draw_industry_assignment(vwret_5n, 'industry5', 5)
