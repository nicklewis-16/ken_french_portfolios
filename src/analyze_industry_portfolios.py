"""
This script visualizes the monthly number of securities by industry and generates summary statistics for both 
replicated and actual data. It supports research and analysis on industry portfolio assignments and their characteristics over time.
The visualizations and statistical summaries are saved to files for further review and inclusion in reports.

Features:
- Draws line plots to show the monthly number of securities across different industries, using distinct colors and linestyles for clarity.
- Generates LaTeX tables of summary statistics for the number of securities, both replicated and actual, for a specified period. These tables are rounded to four decimal places for precision.
- Saves the visualizations as PNG images and the LaTeX tables as .tex files in specified output directories, facilitating easy integration into reports or presentations.

Dependencies:
- pandas: For data manipulation and analysis.
- numpy: For numerical calculations.
- matplotlib: For creating static, interactive, and animated visualizations in Python.
- pathlib: For easy manipulation of paths.
- config: Module containing configuration variables like output and data directory paths.
- datetime: For manipulating dates and times.
- matplotlib.dates: For handling date formats in plots.
- custom modules (load_CRSP_Compustat, load_CRSP_stock, calc_industry_portfolios, test_calc_industry_portfolios): For loading and processing financial data.

"""


import pandas as pd
import numpy as np
from pathlib import Path
import config
from datetime import datetime
from matplotlib.ticker import FuncFormatter
import matplotlib.pyplot as plt
from pandas.plotting import table
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
    fig, ax = plt.subplots(figsize=(12, 12))

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
    ax.set_title("Replicated monthly number of securities by industry")

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
    fig, ax = plt.subplots(figsize=(12, 12))
    ax.plot(num5_firms['1960':])
    ax.set_title("Actual monthly number of securities by industry")
    ax.xaxis.set_major_locator(mdates.YearLocator(10))  # Major ticks every 10 years.
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))  # Format as just the year.
    
    # Rotate and align the tick labels so they look better.
    fig.autofmt_xdate()

    # Use a formatter for the y-axis to ensure commas are used for thousands.
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: '{:,.0f}'.format(x))) 

    # Add a legend.
    ax.legend()

    # Save the figure to a file in the output directory.
    fig.savefig(OUTPUT_DIR / f"actual_sec_per_ind_5.png", dpi=300)
    plt.close(fig)  
    
    summary_stats = vwret_5npiv.describe().round(4)

    # Assuming summary_stats is your DataFrame containing summary statistics
    latex_code = summary_stats.round(4).to_latex(index=True, column_format='lcccccc', float_format="{:0.4f}".format)

    with open(OUTPUT_DIR / "summary_stats_5ind.tex", "w") as file:
        file.write(latex_code)
    
    summary_stats = num5_firms['1960':].describe().round(4)

    # Assuming summary_stats is your DataFrame containing summary statistics
    latex_code = summary_stats.round(4).to_latex(index=True, column_format='lcccccc', float_format="{:0.4f}".format)

    with open(OUTPUT_DIR / "actual_summary_stats_5ind.tex", "w") as file:
        file.write(latex_code)
    