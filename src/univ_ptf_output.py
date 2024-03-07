import pandas as pd
import numpy as np
from pathlib import Path
import config
from plotnine import *
from mizani.formatters import comma_format, percent_format
from datetime import datetime
import matplotlib.pyplot as plt
OUTPUT_DIR = Path(config.OUTPUT_DIR)
DATA_DIR = Path(config.DATA_DIR)

# Function to process each sheet based on its specific column name and structure
def process_sheet_v2(sheet_name, df):
    # Determine category column name based on the sheet name
    if "EP" in sheet_name:
        category_column = 'ep_categories'
    elif "CFP" in sheet_name:
        category_column = 'cfp_categories'
    else:
        # Default to 'ep_categories' if none match
        category_column = 'ep_categories'
    
    # Check if the specified category column exists, if not, use the provided default
    if category_column not in df.columns:
        category_column = df.columns[2]  # Assuming the category column is the third one
    
    # The last column for computation
    last_column = df.columns[-1]
    
    # Split the category column into three new columns
    df[['Category', 'Quantile', 'Decile']] = df[category_column].str.split(',', expand=True)
    
    # Clean the new columns
    df['Category'] = df['Category'].str.strip()
    df['Quantile'] = df['Quantile'].str.strip()
    df['Decile'] = df['Decile'].str.strip()
    
    df.drop(category_column, axis=1, inplace=True)
    
    # Convert 'jdate' to datetime format
    df[df.columns[1]] = pd.to_datetime(df[df.columns[1]])
    
    # Compute averages
    df['Category_Avg'] = df.groupby([df.columns[1], 'Category'])[last_column].transform('mean')
    df['Quantile_Avg'] = df.groupby([df.columns[1], 'Quantile'])[last_column].transform('mean')
    df['Decile_Avg'] = df.groupby([df.columns[1], 'Decile'])[last_column].transform('mean')
    
    # Pivot the table for each category
    df_category_avg = df.pivot_table(index=df.columns[1], columns='Category', values='Category_Avg', aggfunc='mean')
    df_quantile_avg = df.pivot_table(index=df.columns[1], columns='Quantile', values='Quantile_Avg', aggfunc='mean')
    df_decile_avg = df.pivot_table(index=df.columns[1], columns='Decile', values='Decile_Avg', aggfunc='mean')
    
    # Rename columns for clarity
    df_category_avg.columns = [f'Category_{col}' for col in df_category_avg.columns]
    df_quantile_avg.columns = [f'Quantile_{col}' for col in df_quantile_avg.columns]
    df_decile_avg.columns = [f'Decile_{col}' for col in df_decile_avg.columns]
    
    # Merge dataframes on 'jdate'
    df_final = df_category_avg.join([df_quantile_avg, df_decile_avg], how='outer').reset_index()
    
    return df_final

# Read the Excel file
file_path = DATA_DIR/ "manual" /'portfolio_metrics.xlsx'
xls = pd.ExcelFile(file_path, engine='openpyxl')

# Process all sheets in the Excel file with the updated function
dfs_v2 = {}

for sheet_name in xls.sheet_names:
    df = pd.read_excel(xls, sheet_name=sheet_name)
    processed_df = process_sheet_v2(sheet_name, df)
    dfs_v2[sheet_name] = processed_df

for sheet_name, df in dfs_v2.items():
    # Create a range of years from 1952 to 2022
    years = pd.DataFrame({'year': range(1952, 2023)})
    # If the dataframe has more than one column, concatenate years with the rest of the dataframe
    if df.shape[1] > 1:
        updated_df = pd.concat([years, df.iloc[:, 1:].reset_index(drop=True)], axis=1)
    else:
        updated_df = years
    # Update the dictionary with the updated dataframe
    dfs_v2[sheet_name] = updated_df

# Save the processed dataframes into a new Excel file with version 2 modifications
export_path_v2 = OUTPUT_DIR /'portfolio_metrics_final.xlsx'
with pd.ExcelWriter(export_path_v2, engine='openpyxl') as writer:
    for sheet_name, df in dfs_v2.items():
        df.to_excel(writer, sheet_name=sheet_name, index=False)



import pandas as pd

# Assume the processing function process_sheet_v2 is defined as before and available for use

# Read the Excel file
file_path1 = OUTPUT_DIR /'portfolio_metrics_final.xlsx'
xls = pd.ExcelFile(file_path1, engine='openpyxl')

def generate_summary_statistics(dfs):
    # Dictionary to store summary statistics for each processed sheet
    summary_stats_dfs = {}
    
    for sheet_name, df in dfs.items():
        # Ensure the dataframe is suitable for .describe()
        if not df.empty:
            # Generate summary statistics
            summary_stats_df = df.describe()
            # Store the summary statistics DataFrame
            summary_stats_dfs[sheet_name] = summary_stats_df
    
    return summary_stats_dfs

# Assuming 'dfs_v2' is your dictionary of processed DataFrames from each sheet
summary_stats_dfs = generate_summary_statistics(dfs_v2)

# Save the summary statistics DataFrames into a new Excel file
#summary_stats_export_path = OUTPUT_DIR /'Univ_portfolio_summary.xlsx'
#with pd.ExcelWriter(summary_stats_export_path, engine='openpyxl') as writer:
    #for sheet_name, summary_df in summary_stats_dfs.items():
        #summary_df.to_excel(writer, sheet_name=sheet_name)




# Define the sheets and categories to plot
sheets = ['Value Weighted Annual EP', 'Equal Weighted Annual EP', 'Value Weighted Annual CFP', 'Equal Weighted Annual CFP']
categories = ['Category_Hi 30', 'Category_Lo 30', 'Category_Med 40']



file_path1 = OUTPUT_DIR /'portfolio_metrics_final.xlsx'


import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Assuming OUTPUT_DIR is defined in your config and is the base directory for output
# Define the base directory for data, OUTPUT_DIR is assumed to be defined earlier
DATA_DIR = Path(config.DATA_DIR)

# Define the path to save the plots
plot_save_path = DATA_DIR / "data" / "manual"
plot_save_path.mkdir(parents=True, exist_ok=True)

# Sheets and categories of interest
sheets = [
    'Value Weighted Annual EP',
    'Equal Weighted Annual EP',
    'Value Weighted Annual CFP',
    'Equal Weighted Annual CFP'
]
categories = ['Category_Hi 30', 'Category_Lo 30', 'Category_Med 40']

def plot_categories(sheet_name, df, categories, plot_save_path):
    plt.figure(figsize=(10, 6))

    # Check if the 'year' column exists and convert to datetime if necessary
    if 'year' not in df.columns:
        df['year'] = pd.to_datetime(df.iloc[:, 0]).dt.year

    # Plot each category in the provided categories list if it exists in the DataFrame
    for category in categories:
        if category in df.columns:
            plt.plot(df['year'], df[category], label=category)

    plt.title(f'{sheet_name} - Category Comparison')
    plt.xlabel('Year')
    plt.ylabel('Return')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()

    # Save the plot with a dynamic name based on the sheet name
    plt.savefig(plot_save_path / f'{sheet_name.replace(" ", "_")}_category_comparison.png')
    plt.close()

# Plot for each sheet and category
file_path1 = OUTPUT_DIR / 'portfolio_metrics_final.xlsx'
for sheet in sheets:
    # Read the sheet
    df = pd.read_excel(file_path1, sheet_name=sheet)
    # Plot the categories for the sheet
    plot_categories(sheet, df, categories, plot_save_path)