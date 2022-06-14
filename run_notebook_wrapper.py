import subprocess
import sys
from pathlib import Path
import os
import argparse
import zipfile
from typing import Optional
import json

def extract_util_files(zipfile_location: str, extract_folder: str) -> None:
    with zipfile.ZipFile(zipfile_location, "r") as myzip:
        myzip.extractall(extract_folder)

def run_notebook(notebook_path: str, params: Optional[dict]=None) -> str:
    """
    Run a notebook in a subprocess and returns the output file path

    Args:
        notebook_path (str): notebook path
        params (Optional[dict], optional): papermill params to the notebook. Defaults to None.

    Returns:
        str: _description_
    """
    # Run the notebook in a subprocess
    #cmd = f"jupyter nbconvert  --to notebook --inplace --ExecutePreprocessor.timeout=600 --ExecutePreprocessor.kernel_name=python3 --execute {notebook_path}".split()
    output_nb_path = f"{notebook_path.rpartition('.')[0]}_output.ipynb"
    cmd = f"papermill {notebook_path} {output_nb_path} -k python3".split()
    if params is not None:
        for param, val in params.items():
            cmd.extend(["-p", str(param), str(val)])
    subprocess.run(cmd)
    return output_nb_path

def get_html_from_notebook(notebook_path: str) -> str:
    """
    Convert a notebook to html and returns the path of the html file.
    """
    # Convert the notebook to html
    cmd = f"jupyter nbconvert --to html --TemplateExporter.exclude_input=True {notebook_path}".split()
    subprocess.run(cmd)
    # Get the path of the html file
    html_path = notebook_path.replace(".ipynb", ".html")
    return html_path


def list_files(startpath):
    for root, dirs, files in os.walk(startpath):
        level = root.replace(startpath, '').count(os.sep)
        indent = ' ' * 4 * (level)
        print('{}{}/'.format(indent, os.path.basename(root)))
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            print('{}{}'.format(subindent, f))

if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--notebooks_path', type=str, required=True)
    arg_parser.add_argument('--requirements_path', type=str, default="/opt/ml/processing/requirements/requirements.txt")
    arg_parser.add_argument('--output_path', type=str, default="/opt/ml/processing/output")
    arg_parser.add_argument('--utils_path', type=str)
    arg_parser.add_argument("--utils_extract_to", type=str)
    arg_parser.add_argument("--nb_parameters", type=json.loads)
    print(list_files('/opt/ml/processing/'))
    args = arg_parser.parse_args()
    # Install requirements
    subprocess.run([sys.executable, "-m", "pip", "install", "-r", args.requirements_path, "--quiet"])
    if args.utils_path:
        extract_util_files(os.path.join(args.utils_path, "utils.zip"), args.utils_extract_to)
    print(list_files('/opt/ml/processing/'))
    output_notebook_path = run_notebook(args.notebooks_path, args.nb_parameters)
    # Generate html file
    html_path = get_html_from_notebook(output_notebook_path)
    Path(args.output_path).mkdir(parents=True, exist_ok=True)
    # Copy html file to output path
    subprocess.run(f"mv {html_path} {os.path.join(args.output_path, html_path.split('/')[-1])}".split())
