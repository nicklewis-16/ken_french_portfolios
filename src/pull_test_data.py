
"""
This script fetches data for various portfolios from the Ken French Data Library website, saves the data to Excel files,
and downloads and extracts ZIP files for specific portfolios of interest (operating profitability and investment). 
Due to operating profitability and investment issues with pandas data reader, we use beautiful soup to 
scrape the data from the website and download the data as zip files. Further parsing after this is needed to get these files
to the same format as all the other portfolios (description in sheet 1, data in subsequent sheets).

The script consists of the following main functions:

1. save_portfolio_data_to_excel(portfolio_descriptions):
    - Fetches data for each portfolio from the Fama-French data library using pandas_datareader.
    - Saves the data to an Excel file, with descriptions and data in separate sheets.

2. scrape_and_download(base_url, filedir, portfolios):
    - Scrapes the webpage content from the given base URL using BeautifulSoup.
    - Filters the relevant links for ZIP files.
    - Downloads and extracts the ZIP files for the portfolios of interest.

3. download_and_extract_zip(url, path, portfolio_name):
    - Downloads a ZIP file from the specified URL.
    - Saves the ZIP file to the specified path.
    - Extracts the contents of the ZIP file to the same path.

The script also defines the following variables:
- portfolio_descriptions: A dictionary containing portfolio names as keys and tuples of start and end dates as values.
- base_url: The base URL of the webpage to scrape.
- portfolios: A list of portfolio names to filter the links.
- filedir: The directory where the downloaded files will be saved.

To run the script, execute the main block at the end of the file.
"""
import pandas_datareader.data as web
import pandas as pd
from pathlib import Path
import config
import warnings
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import zipfile
warnings.filterwarnings("ignore")


bivariate_ep = ('1951-07-01', '2023-12-31')
bivariate_cfp = ('1951-07-01', '2023-12-31')
bivariate_div = ('1927-07-01', '2023-12-31')
op_inv = ('1963-07-01', '2023-12-31')
industry = ('1926-07-01', '2023-12-31')

portfolio_descriptions = {'6_Portfolios_ME_EP_2x3': bivariate_ep,
                          '6_Portfolios_ME_EP_2x3_Wout_Div': bivariate_ep,
                          '6_Portfolios_ME_CFP_2x3': bivariate_cfp,
                          '6_Portfolios_ME_CFP_2x3_Wout_Div': bivariate_cfp,
                          '6_Portfolios_ME_DP_2x3': bivariate_div,
                          '6_Portfolios_ME_DP_2x3_Wout_Div': bivariate_div,
                          '5_Industry_Portfolios': industry,
                          '5_Industry_Portfolios_Wout_Div': industry,
                          '5_Industry_Portfolios_daily': industry,
                          '49_Industry_Portfolios': industry,
                          '49_Industry_Portfolios_Wout_Div': industry,
                          '49_Industry_Portfolios_daily': industry
                          }

op_inv_ports = {'25_Portfolios_OP_INV_5x5': op_inv,
                '25_Portfolios_OP_INV_5x5_Wout_Div': op_inv,
                '25_Portfolios_OP_INV_5x5_daily': op_inv}

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

            # Extract the ZIP file
            with zipfile.ZipFile(file_path, "r") as zip_ref:
                zip_ref.extractall(path)
            print(f"Download and extraction complete for {portfolio_name}")
        else:
            print(f"Failed to download file. HTTP status code: {response.status_code}")
    except requests.RequestException as e:
        print(f"Failed to download {url}. Error: {e}")
    except FileNotFoundError as e:
        print(f"File not found during extraction: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")



            
if __name__ == "__main__":
    save_portfolio_data_to_excel(portfolio_descriptions)
    scrape_and_download(base_url, filedir, portfolios)