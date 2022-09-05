"""
Extract-transform-load the already downloaded data.
Datasets should already be present before running this script. See: scr/data/download_data.py to download.
"""

# TODO ETL entire script

from config import proj
from pathlib import Path
from os import listdir
from src.utils.etl_utils import *

PATH_RAW_DATA_DIR = Path(proj.proj_paths["top"]).joinpath('data').joinpath('raw')
PATH_INTERIM_DATA_DIR = Path(proj.proj_paths["top"]).joinpath('data').joinpath('interim')
PATH_PROC_DATA_DIR = Path(proj.proj_paths["top"]).joinpath('data').joinpath('processed')

# TODO Decomp main file
decomp_file(str(PATH_RAW_DATA_DIR.joinpath('favorita-grocery-sales-forecasting.zip')), str(PATH_INTERIM_DATA_DIR))

# TODO loop through each and create a parquet file
for file_path in listdir(PATH_INTERIM_DATA_DIR):
    decomp_file(file_path, PATH_INTERIM_DATA_DIR)
    get_schema()
    csv_to_parquet()

# TODO Clean up interim

