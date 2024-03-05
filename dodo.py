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
        "verbosity": 2, # Print everything immediately. This is important in
        # case WRDS asks for credentials.
    }


def task_pull_CRSP_Compustat():
    """Pull CRSP/Compustat data from WRDS and save to disk
    """
    file_dep = [
        "./src/config.py", 
        "./src/load_CRSP_stock.py",
        "./src/load_CRSP_Compustat.py",
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
        ]
    ]

    return {
        "actions": [
            "ipython src/config.py",
            "ipython src/load_CRSP_Compustat.py",
            "ipython src/load_CRSP_stock.py",
        ],
        "targets": targets,
        "file_dep": file_dep,
        "clean": True,
        "verbosity": 2, # Print everything immediately. This is important in
        # case WRDS asks for credentials.
    }

# def task_summary_stats():
#     """ """
#     file_dep = ["./src/example_table.py"]
#     file_output = [
#         "example_table.tex",
#         "pandas_to_latex_simple_table1.tex",
#         ]
#     targets = [OUTPUT_DIR / file for file in file_output]

#     return {
#         "actions": [
#             "ipython ./src/example_table.py",
#             "ipython ./src/pandas_to_latex_demo.py",
#         ],
#         "targets": targets,
#         "file_dep": file_dep,
#         "clean": True,
#     }


# def task_example_plot():
#     """Example plots"""
#     file_dep = [Path("./src") / file for file in ["example_plot.py", "load_fred.py"]]
#     file_output = ["example_plot.png"]
#     targets = [OUTPUT_DIR / file for file in file_output]

#     return {
#         "actions": [
#             "ipython ./src/example_plot.py",
#         ],
#         "targets": targets,
#         "file_dep": file_dep,
#         "clean": True,
#     }


def task_convert_notebooks_to_scripts():
    """Preps the notebooks for presentation format.
    Execute notebooks with summary stats and plots and remove metadata.
    """
    build_dir = Path(OUTPUT_DIR)
    build_dir.mkdir(parents=True, exist_ok=True)

    notebooks = [
        "01_example_notebook.ipynb",
        "02_interactive_plot_example.ipynb",
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
        "01_example_notebook.ipynb",
        "02_interactive_plot_example.ipynb",
    ]
    stems = [notebook.split(".")[0] for notebook in notebooks]

    file_dep = [
        # 'load_other_data.py',
        *[Path(OUTPUT_DIR) / f"_{stem}.py" for stem in stems],
    ]

    targets = [
        ## 01_example_notebook.ipynb output
        OUTPUT_DIR / "sine_graph.png",
        ## Notebooks converted to HTML
        *[OUTPUT_DIR / f"{stem}.html" for stem in stems],
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


# def task_knit_RMarkdown_files():
#     """Preps the RMarkdown files for presentation format.
#     This will knit the RMarkdown files for easier sharing of results.
#     """
#     files_to_knit = [
#         'shift_share.Rmd',
#         ]

#     files_to_knit_stems = [file.split('.')[0] for file in files_to_knit]

#     file_dep = [
#         'load_performance_and_loan_merged.py',
#         *[file + ".Rmd" for file in files_to_knit_stems],
#         ]

#     file_output = [file + '.html' for file in files_to_knit_stems]
#     targets = [OUTPUT_DIR / file for file in file_output]

#     def knit_string(file):
#         return f"""Rscript -e 'library(rmarkdown); rmarkdown::render("{file}.Rmd", output_format="html_document", OUTPUT_DIR="../output/")'"""
#     actions = [knit_string(file) for file in files_to_knit_stems]
#     return {
#         "actions": [
#                     "module use -a /opt/aws_opt/Modulefiles",
#                     "module load R/4.2.2",
#                     *actions],
#         "targets": targets,
#         'task_dep':[],
#         "file_dep": file_dep,
#     }


def task_compile_latex_docs():
    """Example plots"""
    file_dep = [
        "./reports/report_example.tex",
        "./reports/slides_example.tex",
        "./src/example_plot.py",
        "./src/example_table.py",
    ]
    file_output = [
        "./reports/report_example.pdf",
        "./reports/slides_example.pdf",
    ]
    targets = [file for file in file_output]

    return {
        "actions": [
            "latexmk -xelatex -cd ./reports/report_example.tex",  # Compile
            "latexmk -xelatex -c -cd ./reports/report_example.tex",  # Clean
            "latexmk -xelatex -cd ./reports/slides_example.tex",  # Compile
            "latexmk -xelatex -c -cd ./reports/slides_example.tex",  # Clean
            # "latexmk -CA -cd ../reports/",
        ],
        "targets": targets,
        "file_dep": file_dep,
        "clean": True,
    }

