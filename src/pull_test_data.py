import pandas_datareader.data as web
import pandas as pd
from pathlib import Path
import config
import warnings
from pathlib import Path

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
                          #'25_Portfolios_OP_INV_5x5': op_inv,
                          #'25_Portfolios_OP_INV_5x5_Wout_Div': op_inv,
                          #'25_Portfolios_OP_INV_5x5_daily': op_inv,
                          '5_Industry_Portfolios': industry,
                          '5_Industry_Portfolios_Wout_Div': industry,
                          '5_Industry_Portfolios_daily': industry,
                          '49_Industry_Portfolios': industry,
                          '49_Industry_Portfolios_Wout_Div': industry,
                          '49_Industry_Portfolios_daily': industry
                          }

filedir = Path(config.OUTPUT_DIR) / 'famafrench'
filedir.mkdir(parents=True, exist_ok=True)

def save_portfolio_data_to_excel(portfolio_descriptions):

    """
    Fetches data for each portfolio and saves it to an Excel file, with descriptions and data in separate sheets.
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
            
if __name__ == "__main__":
    save_portfolio_data_to_excel(portfolio_descriptions)