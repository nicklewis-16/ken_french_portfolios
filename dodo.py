"""Run or update the project. This file uses the `doit` Python package. It works
like a Makefile, but is Python-based
"""
import sys
sys.path.insert(1, './src/')


import config
from pathlib import Path
from doit.tools import run_once
import platform

OUTPUT_DIR = Path(config.OUTPUT_DIR)
DATA_DIR = Path(config.DATA_DIR)



def jupyter_execute_notebook(notebook):
    return f"jupyter nbconvert --execute --to notebook --ClearMetadataPreprocessor.enabled=True --inplace ./src/{notebook}.ipynb"
def jupyter_to_html(notebook, output_dir=OUTPUT_DIR):
    return f"jupyter nbconvert --to html --output-dir={output_dir} ./src/{notebook}.ipynb"
def jupyter_to_md(notebook, output_dir=OUTPUT_DIR):
    """Requires jupytext"""
    return f"jupytext --to markdown --output-dir={output_dir} ./src/{notebook}.ipynb"
def jupyter_to_python(notebook, build_dir):
    """Convert a notebook to a python script"""
    return f"jupyter nbconvert --to python ./src/{notebook}.ipynb --output _{notebook}.py --output-dir {build_dir}"
def jupyter_clear_output(notebook):
    return f"jupyter nbconvert --ClearOutputPreprocessor.enabled=True --ClearMetadataPreprocessor.enabled=True --inplace ./src/{notebook}.ipynb"
# fmt: on

def get_os():
    os_name = platform.system()
    if os_name == "Windows":
        return "windows"
    elif os_name == "Darwin":
        return "nix"
    elif os_name == "Linux":
        return "nix"
    else:
        return "unknown"
os_type = get_os()

def copy_notebook_to_folder(notebook_stem, origin_folder, destination_folder):
    origin_path = Path(origin_folder) / f"{notebook_stem}.ipynb"
    destination_path = Path(destination_folder) / f"_{notebook_stem}.ipynb"
    if os_type == "nix":
        command =  f"cp {origin_path} {destination_path}"
    else:
        command = f"copy  {origin_path} {destination_path}"
    return command

    
def task_pull_ken_french_data():
    """Pull CRSP/Compustat data from WRDS and save to disk
    """
    file_dep = [
        "./src/config.py", 
        "./src/pull_test_data.py"
        ]
    targets = [
        Path(DATA_DIR) / "famafrench" / file for file in 
        [
            ## src/pull_test_data.py
            "5_Industry_Portfolios_daily.xlsx",
            "5_Industry_Portfolios_Wout_Div.xlsx",
            "5_Industry_Portfolios.xlsx",
            "6_Portfolios_ME_CFP_2x3_Wout_Div.xlsx", 
            "6_Portfolios_ME_CFP_2x3.xlsx",
            "6_Portfolios_ME_DP_2x3_Wout_Div.xlsx",
            "6_Portfolios_ME_DP_2x3.xlsx",
            "6_Portfolios_ME_EP_2x3_Wout_Div.xlsx",
            "6_Portfolios_ME_EP_2x3.xlsx",
            "25_Portfolios_OP_INV_5x5_daily.xlsx",
            "25_Portfolios_OP_INV_5x5_Wout_Div.xlsx",
            "25_Portfolios_OP_INV_5x5.xlsx",
            "49_Industry_Portfolios_daily.xlsx",
            "49_Industry_Portfolios_Wout_Div.xlsx",
            "49_Industry_Portfolios.xlsx",
            "Portfolios_Formed_on_CF-P_Wout_Div.xlsx",
            "Portfolios_Formed_on_CF-P.xlsx",
            "Portfolios_Formed_on_D-P_Wout_Div.xlsx",
            "Portfolios_Formed_on_D-P.xlsx",
            "Portfolios_Formed_on_E-P_Wout_Div.xlsx",
            "Portfolios_Formed_on_E-P.xlsx"
        ]
    ]

    return {
        "actions": [
            "ipython src/config.py",
            "ipython src/pull_test_data.py",
        ],
        "targets": targets,
        "file_dep": file_dep,
        "clean": True,
        "verbosity": 2,
    }


def task_pull_CRSP_Compustat():
    """Pull CRSP/Compustat data from WRDS and save to disk
    """
    file_dep = [
        "./src/config.py", 
        "./src/load_CRSP_stock.py",
        "./src/load_CRSP_Compustat.py",
        "./src/load_CRSP_stock_v2.py",
        "./src/load_CRSP_Compustat_v2.py",
        ]
    targets = [
        Path(DATA_DIR) / "pulled" / file for file in 
        [
            ## src/load_CRSP_Compustat.py
            "Compustat.parquet",
            "CRSP_stock.parquet",
            "CRSP_Comp_Link_Table.parquet",
            ## src/load_CRSP_stock.py
            "CRSP_MSF_INDEX_INPUTS.parquet", 
            "CRSP_MSIX.parquet", 
            ## src/load_CRSP_Compustat_v2.py
            "v2/Compustat.parquet",
            "v2/CRSP_stock.parquet",
            "v2/CRSP_Comp_Link_Table.parquet",
            ## src/load_CRSP_stock_v2.py
            "v2/CRSP_MSF_INDEX_INPUTS.parquet", 
            "v2/CRSP_MSIX.parquet", 
        ]
    ]

    return {
        "actions": [
            "ipython src/config.py",
            "ipython src/load_CRSP_Compustat.py",
            "ipython src/load_CRSP_stock.py",
            "ipython src/load_CRSP_Compustat_v2.py",
            "ipython src/load_CRSP_stock_v2.py",
        ],
        "targets": targets,
        "file_dep": file_dep,
        "clean": True,
        "verbosity": 2, # Print everything immediately. This is important in
        # case WRDS asks for credentials.
    }



def task_calc_industries():
    """calculates the industry portfolios
    """
    file_dep = [
        "./src/config.py", 
        "./src/load_CRSP_stock.py",
        "./src/load_CRSP_Compustat.py",
        ]
    targets = [
        Path(DATA_DIR) / "manual" / file for file in 
        [
            ## src/calc_industry_portfolios.py
            "5industry_portfolios.xlsx",
            "49industry_portfolios.xlsx"
        ]
    ]

    return {
        "actions": [
            "ipython src/calc_industry_portfolios.py",
        ],
        "targets": targets,
        "file_dep": file_dep,
        "clean": True,
        "verbosity": 2, 
    }

def task_analyze_industries():
    """calculates the industry portfolios
    """
    file_dep = [
        "./src/calc_industry_portfolios.py",
        ]
    targets = [
        Path(OUTPUT_DIR) / file for file in 
        [
            ## src/calc_industry_portfolios.py
            "sec_per_ind_5.png",
            "actual_sec_per_ind_5.png",
            "summary_stats_5ind.tex",
            'actual_summary_stats_5ind.tex',
        ]
    ]

    return {
        "actions": [
            "ipython src/analyze_industry_portfolios.py",
        ],
        "targets": targets,
        "file_dep": file_dep,
        "clean": True,
        "verbosity": 2, 
    }



def task_calc_inv_op():
    """calculates the inv op portfolios
    """
    file_dep = [
        "./src/config.py", 
        "./src/load_CRSP_stock.py",
        "./src/load_CRSP_Compustat.py",
        ]
    targets = [
        Path(DATA_DIR) / "manual" / file for file in 
        [
            ## src/calc_op_inv_portfolios.py
            "5x5_OP_INV_portfolios.xlsx",
        ]
    ]

    return {
        "actions": [
            "ipython src/calc_op_inv_portfolios.py",
        ],
        "targets": targets,
        "file_dep": file_dep,
        "clean": True,
        "verbosity": 2, 
    }
    
def task_analyze_op_inv():
    """calculates the inv_op portfolios
    """
    file_dep = [
        "./src/calc_op_inv_portfolios.py",
        ]
    targets = [
        Path(OUTPUT_DIR) / file for file in 
        [
            ## src/analyze_OP_INV_portfolios.py
            'output/opinv_mixed.png',
            'output/opinv_same.png'
        ]
    ]

    return {
        "actions": [
            "ipython src/analyze_OP_INV_portfolios.py",
        ],
        "targets": targets,
        "file_dep": file_dep,
        "clean": True,
        "verbosity": 2, 
    }
    
def task_calc_univ_portfolios_temp():
    """calculates the univariate portfolios
    """
    file_dep = [
        "./src/config.py", 
        "./src/load_CRSP_stock_v2.py",
        "./src/load_CRSP_Compustat_v2.py",
        ]
    targets = [
        Path(DATA_DIR) / "manual" / file for file in 
        [
            "portfolio_metrics.xlsx",
        ]
    ]

    return {
        "actions": [
            "ipython src/calc_univariate_portfolios.py",
        ],
        "targets": targets,
        "file_dep": file_dep,
        "clean": True,
        "verbosity": 2, 
    }


def task_calc_univ_portfolios():
    """calculates the univariate portfolios
    """
    file_dep = [
        "./src/calc_univariate_portfolios.py"
        ]
    targets = [
        Path(OUTPUT_DIR) / file for file in 
        [
            ## src/calc_op_inv_portfolios.py
            "portfolio_metrics_final.xlsx",
        ]
    ]

    return {
        "actions": [
            "ipython src/univ_ptf_output.py",
        ],
        "targets": targets,
        "file_dep": file_dep,
        "clean": True,
        "verbosity": 2, 
    }


def task_compile_latex():
    return {
        'actions': ['pdflatex -interaction=nonstopmode main.tex'],
        'file_dep': ['main.tex'],  # Add dependencies like your .tex file and any figures or included files
        'targets': ['main.pdf'],  # The expected output PDF file
        'clean': True,  # Clean command will remove the target
    }

def task_convert_notebooks_to_scripts():
    """Preps the notebooks for presentation format.
    Execute notebooks with summary stats and plots and remove metadata.
    """
    build_dir = Path(OUTPUT_DIR)
    build_dir.mkdir(parents=True, exist_ok=True)

    notebooks = [
        "step_by_step.ipynb",
    ]
    file_dep = [Path("./src") / file for file in notebooks]
    stems = [notebook.split(".")[0] for notebook in notebooks]
    targets = [build_dir / f"_{stem}.py" for stem in stems]

    actions = [
        # *[jupyter_execute_notebook(notebook) for notebook in notebooks_to_run],
        # *[jupyter_to_html(notebook) for notebook in notebooks_to_run],
        *[jupyter_clear_output(notebook) for notebook in stems],
        *[jupyter_to_python(notebook, build_dir) for notebook in stems],
    ]
    return {
        "actions": actions,
        "targets": targets,
        "task_dep": [],
        "file_dep": file_dep,
        "clean": True,
    }


def task_run_notebooks():
    """Preps the notebooks for presentation format.
    Execute notebooks with summary stats and plots and remove metadata.
    """
    notebooks = [
        "step_by_step.ipynb",
    ]
    stems = [notebook.split(".")[0] for notebook in notebooks]

    file_dep = [
        # 'load_other_data.py',
        *[Path(OUTPUT_DIR) / f"_{stem}.py" for stem in stems],
    ]

    targets = [
        ## 01_example_notebook.ipynb output
        # OUTPUT_DIR / "sine_graph.png",
        # ## Notebooks converted to HTML
        # *[OUTPUT_DIR / f"{stem}.html" for stem in stems],
    ]

    actions = [
        *[jupyter_execute_notebook(notebook) for notebook in stems],
        *[jupyter_to_html(notebook) for notebook in stems],
        *[copy_notebook_to_folder(notebook, Path("./src"), OUTPUT_DIR) for notebook in stems],
        *[copy_notebook_to_folder(notebook, Path("./src"), "./docs") for notebook in stems],
        *[jupyter_clear_output(notebook) for notebook in stems],
        # *[jupyter_to_python(notebook, build_dir) for notebook in notebooks_to_run],
    ]
    return {
        "actions": actions,
        "targets": targets,
        "task_dep": [],
        "file_dep": file_dep,
        "clean": True,
    }



# def task_compile_latex_docs():
#     """Example plots"""
#     file_dep = [
#         "./reports/report_example.tex",
#         "./reports/slides_example.tex",
#         "./src/example_plot.py",
#         "./src/example_table.py",
#     ]
#     file_output = [
#         "./reports/main.pdf",
#     ]
#     targets = [file for file in file_output]

#     return {
#         "actions": [
#             "latexmk -xelatex -cd ./reports/main.tex",  # Compile
#             "latexmk -xelatex -c -cd ./reports/main.tex",  # Clean
#             "latexmk -xelatex -cd ./reports/slides_example.tex",  # Compile
#             "latexmk -xelatex -c -cd ./reports/slides_example.tex",  # Clean
#             # "latexmk -CA -cd ../reports/",
#         ],
#         "targets": targets,
#         "file_dep": file_dep,
#         "clean": True,
#     }

