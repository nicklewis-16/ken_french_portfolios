"""
This script fetches data for various portfolios from the Ken French Data Library,
saves the data to Excel files, and downloads and extracts ZIP files for specific portfolios.

The script consists of the following functions:
- save_portfolio_data_to_excel: Fetches data for each portfolio and saves it to an Excel file.
- scrape_and_download: Scrapes the webpage content, filters relevant links, and downloads and extracts ZIP files.
- download_and_extract_zip: Downloads a ZIP file, saves it to a specified path, and extracts its contents.
- op_inv_ports_to_dfs: Converts operating performance and investment portfolios CSV into a list of pandas DataFrames.
"""

import pandas_datareader.data as web
import pandas as pd
from pathlib import Path
import config
import warnings
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import zipfile
import csv
warnings.filterwarnings("ignore")


bivariate_ep = ('1951-07-01', '2023-12-31')
bivariate_cfp = ('1951-07-01', '2023-12-31')
bivariate_div = ('1927-07-01', '2023-12-31')
op_inv = ('1963-07-01', '2023-12-31')
industry = ('1926-07-01', '2023-12-31')

portfolio_descriptions = {'Portfolios_Formed_on_E-P': bivariate_ep, #UNIVARIATE SORTS 
                          'Portfolios_Formed_on_E-P_Wout_Div': bivariate_ep,#UNIVARIATE SORTS 
                          'Portfolios_Formed_on_CF-P': bivariate_cfp,#UNIVARIATE SORTS 
                          'Portfolios_Formed_on_CF-P_Wout_Div': bivariate_cfp,#UNIVARIATE SORTS 
                          'Portfolios_Formed_on_D-P': bivariate_div,#UNIVARIATE SORTS 
                          'Portfolios_Formed_on_D-P_Wout_Div': bivariate_div, #UNIVARIATE SORTS 
                          '6_Portfolios_ME_EP_2x3': bivariate_ep, #BIVARIATE SORTS 
                          '6_Portfolios_ME_EP_2x3_Wout_Div': bivariate_ep,#BIVARIATE SORTS 
                          '6_Portfolios_ME_CFP_2x3': bivariate_cfp,#BIVARIATE SORTS 
                          '6_Portfolios_ME_CFP_2x3_Wout_Div': bivariate_cfp,#BIVARIATE SORTS 
                          '6_Portfolios_ME_DP_2x3': bivariate_div,#BIVARIATE SORTS 
                          '6_Portfolios_ME_DP_2x3_Wout_Div': bivariate_div,#BIVARIATE SORTS 
                          '5_Industry_Portfolios': industry,
                          '5_Industry_Portfolios_Wout_Div': industry,
                          '5_Industry_Portfolios_daily': industry,
                          '49_Industry_Portfolios': industry,
                          '49_Industry_Portfolios_Wout_Div': industry,
                          '49_Industry_Portfolios_daily': industry
                          }

op_inv_ports = {'25_Portfolios_OP_INV_5x5': f'../data/famafrench/ftp/25_Portfolios_OP_INV_5x5.csv',
                '25_Portfolios_OP_INV_5x5_Wout_Div': f'../data/famafrench/ftp/25_Portfolios_OP_INV_5x5_Wout_Div.csv',
                '25_Portfolios_OP_INV_5x5_daily':f'../data/famafrench/ftp/25_Portfolios_OP_INV_5x5_daily.csv'}

# Configuration
base_url = 'https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html'

# Specific portfolios of interest for operating profitability and investment
portfolios = list(op_inv_ports.keys())

filedir = Path(config.DATA_DIR) / 'famafrench'
filedir.mkdir(parents=True, exist_ok=True)

def save_portfolio_data_to_excel(portfolio_descriptions):
    """
    Fetches data for each portfolio and saves it to an Excel file, with descriptions and data in separate sheets.

    Args:
        portfolio_descriptions (dict): A dictionary containing portfolio names as keys and tuples of start and end dates as values.

    Returns:
        None
    """
    for portfolio_name, (start_date, end_date) in portfolio_descriptions.items():
        # Fetch data for the portfolio
        data = web.DataReader(portfolio_name, 'famafrench', start=start_date, end=end_date)
        
        # Define the Excel file path
        excel_path = filedir / f"{portfolio_name.replace('/', '_')}.xlsx"  # Ensure the name is file-path friendly
        with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
            # Write the description to the first sheet
            if 'DESCR' in data:
                description_df = pd.DataFrame([data['DESCR']], columns=['Description'])
                description_df.to_excel(writer, sheet_name='Description', index=False)
            
            # Write each table in the data to subsequent sheets
            for table_key, df in data.items():
                if table_key == 'DESCR':
                    continue  # Skip the description since it's already handled
                sheet_name = str(table_key)  # Naming sheets by their table_key
                df.to_excel(writer, sheet_name=sheet_name[:31])  # Sheet name limited to 31 characters
            
def scrape_and_download(base_url, filedir, portfolios):
    """
    Scrapes the webpage content from the given base URL, filters the relevant links,
    and downloads and extracts the ZIP files for the portfolios of interest.

    Args:
        base_url (str): The base URL of the webpage to scrape.
        filedir (str): The directory where the downloaded files will be saved.
        portfolios (list): A list of portfolio names to filter the links.

    Returns:
        None
    """
    # Fetch the webpage content
    response = requests.get(base_url)
    if response.status_code != 200:
        print(f"Failed to fetch {base_url}")
        return
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Filter the <a> tags to find relevant links
    links = soup.find_all('a', href=True)
    for link in links:
        href = link.get('href')
        if href and "CSV.zip" in href:
            # Check if the link matches any portfolios of interest
            portfolio_name = href.split("_CSV.zip")[0]
            if any(portfolio in portfolio_name for portfolio in portfolios):
                full_url = urljoin(base_url, href)
                print(f"Found: {full_url}")
                download_and_extract_zip(full_url, filedir, portfolio_name)

def download_and_extract_zip(url, path, portfolio_name):
    """
    Downloads a ZIP file from the specified URL, saves it to the specified path,
    and extracts its contents to the same path.

    Args:
        url (str): The URL of the ZIP file to download.
        path (str): The path where the ZIP file will be saved and extracted.
        portfolio_name (str): The name of the portfolio.

    Returns:
        None
    """
    print(f"Downloading and extracting {url}...")
    try:
        # Ensure the target directory exists
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)

        # Define the full path for the ZIP file
        file_path = path / f"{portfolio_name}.CSV.zip"

        # Download the file
        response = requests.get(url)
        if response.status_code == 200:
            print(f"Download successful: {url}")
            # Ensure the directory for the ZIP file exists
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save the ZIP file to the specified directory
            with open(file_path, "wb") as file:
                file.write(response.content)
            print(f"File saved successfully: {file_path}")

            with zipfile.ZipFile(file_path, "r") as zip_ref:
                # Iterate through each file in the ZIP
                for file_info in zip_ref.infolist():
                    with zip_ref.open(file_info) as file:
                        # Read as binary, then decode
                        content = file.read().decode('latin-1')  # Adjust the decoding as necessary
                        # Write the decoded content to a new file, ensuring UTF-8 encoding
                        with open(path / 'ftp' / file_info.filename, "w", encoding="utf-8") as output_file:
                            output_file.write(content)
            print(f"Download and extraction complete for {portfolio_name}")
        else:
            print(f"Failed to download file. HTTP status code: {response.status_code}")
    except requests.RequestException as e:
        print(f"Failed to download {url}. Error: {e}")
    except FileNotFoundError as e:
        print(f"File not found during extraction: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


def op_inv_ports_to_dfs(file_path):
    """
    Converts operational and investment portfolios CSV into a list of pandas DataFrames.
    The first DataFrame in the list contains a single cell with concatenated metadata from the first 22 lines.
    
    Args:
        file_path (str): File path to the CSV containing the operational and investment portfolio data.
    Returns:
        list of pd.DataFrame: A list where the first DataFrame contains concatenated metadata, 
                              and each subsequent DataFrame represents a segmented table from the file.
    """
    # Read and concatenate the first 22 lines into a single string
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file)
        first_22_lines = [next(reader) for _ in range(22)]
        concatenated_string = '\n'.join([','.join(line) for line in first_22_lines])
    
    description_sheets = '\n         0:   Average Value Weighted Returns -- Monthly\n \
        1:   Average Equal Weighted Returns -- Monthly\n \
        2:   Average Value Weighted Returns -- Annual\n \
        3:   Average Equal Weighted Returns -- Annual\n \
        4:   Number of Firms in Portfolios\n \
        5:   Average Market Cap\n \
        6:   For portfolios formed in June of year t. \n              Value Weight Average of BE/ME Calculated for June of t to June of t+1 as:\n              Sum[ME(Mth) * BE(Fiscal Year t-1) / ME(Dec t-1)] / Sum[ME(Mth)]\n              Where Mth is a month from June of t to June of t+1 and BE(Fiscal Year t-1) is adjusted for net stock issuance to Dec t-1\n \
        7:   For portfolios formed in June of year t. \n              Value Weight Average of BE_FYt-1/ME_June t Calculated for June of t to June of t+1 as:\n              Sum[ME(Mth) * BE(Fiscal Year t-1) / ME(Jun t)] / Sum[ME(Mth)] \n              Where Mth is a month from June of t to June of t+1 \n              and BE(Fiscal Year t-1) is adjusted for net stock issuance to Jun t \n \
        8:   For portfolios formed in June of year t. \n              Value Weight Average of OP Calculated as: \n              Sum[ME(Mth) * OP(fiscal year t-1) / BE(fiscal year t-1)] / Sum[ME(Mth)]\n              Where Mth is a month from June of t to June of t+1 \n \
        9:   For portfolios formed in June of year t. \n              Value Weight Average of investment (rate of growth of assets) Calculated as: \n              Sum[ME(Mth) * Log(ASSET(t-1) / ASSET(t-2) / Sum[ME(Mth)]\n              Where Mth is a month from June of t to June of t+1   \n \
        '
    description_daily = '\nIF DAILY\n         0:   Average Value Weighted Returns -- Daily\n \
        1:   Average Equal Weighted Returns -- Daily\n \
        2:   Number of Firms in Portfolios\n \
        3:   Average Firm Size\n '
    concatenated_string += description_sheets
    concatenated_string += description_daily
    # Create a DataFrame from the concatenated string
    metadata_df = pd.DataFrame([concatenated_string], columns=['Description'])
    
    # Now, read the rest of the file into a DataFrame, skipping the first 22 lines
    df = pd.read_csv(file_path, encoding='utf-8', skiprows=22)
    df = df.reset_index()
    
    # Identify separators (blank rows) and segment the DataFrame
    separator_indices = df.index[df.iloc[:, 1:].isna().all(axis=1)].tolist()
    dfs = [metadata_df]  # Initialize the list of DataFrames with the metadata DataFrame
    start_idx = 0  # Start index for slicing
    
    for end_idx in separator_indices:
        segment = df.iloc[start_idx:end_idx]
        if not segment.empty:
            dfs.append(segment)
        start_idx = end_idx + 1
    
    if start_idx < len(df):
        dfs.append(df.iloc[start_idx:])
    
    # Process each segment except the first (metadata) DataFrame
    for i in range(1, len(dfs)):
        df_segment = dfs[i]
        df_segment.columns = df_segment.iloc[0]  # Set the first row as column names
        dfs[i] = df_segment[1:].reset_index(drop=True)  # Remove the first row
        
        # Apply date parsing and other operations
        dfs[i].iloc[:, 0] = dfs[i].iloc[:, 0].apply(parse_date)
        dfs[i].set_index(dfs[i].columns[0], inplace=True)
        dfs[i].index.name = 'Date'
        dfs[i] = dfs[i].apply(pd.to_numeric, errors='coerce')
        dfs[i].columns.name = ''
    
    return dfs

def parse_date(x):
    x_str = str(x).strip()
    try:
        if len(x_str) == 4:
            return pd.to_datetime(x_str + '0101', format='%Y%m%d')
        elif len(x_str) == 6:
            return pd.to_datetime(x_str, format='%Y%m')
        elif len(x_str) == 8:
            return pd.to_datetime(x_str, format='%Y%m%d')
        else:
            return pd.NaT
    except ValueError:
        return pd.NaT


def write_dfs_to_excel(file_path, excel_file_name):
    """
    Processes a given CSV file into segmented DataFrames and writes them to an Excel workbook.
    
    Args:
        file_path (str): Path to the source CSV file.
        excel_file_name (str): Name of the Excel file to be created.
    """
    dfs = op_inv_ports_to_dfs(file_path)  # Assuming op_inv_ports_to_dfs is your processing function
    
    # Define the path for the Excel file
    excel_file_path = f'../data/famafrench/{excel_file_name}.xlsx'
    
    with pd.ExcelWriter(excel_file_path, engine='xlsxwriter') as writer:
        dfs[0].to_excel(writer, sheet_name='Description')
        for i, df_segment in enumerate(dfs[1:], start=0):
            sheet_name = str(i)
            df_segment.to_excel(writer, sheet_name=sheet_name)            

            
if __name__ == "__main__":
    save_portfolio_data_to_excel(portfolio_descriptions)
    scrape_and_download(base_url, filedir, portfolios)
    for portfolio_name, file_path in op_inv_ports.items():
        write_dfs_to_excel(file_path, portfolio_name)