"""
Extract-transform-load the already downloaded data.
Datasets should already be present before running this script. See: scr/data/download_data.py to download.
"""

from config import proj
from src.utils.etl_utils import *
from re import sub
from pathlib import Path

# Clear interm data directory for clean run
clear_interm_data_dir()

# Decompress downloaded file (main file from Kaggle)
decomp_file(file_path=proj.Config.paths.get("data_raw").joinpath('favorita-grocery-sales-forecasting.zip'),
            extract_path=proj.Config.paths.get("data_interim"))

# Get file list post decompression
file_list = get_file_list(dir_path=proj.Config.paths.get("data_interim"),
                          regex_etx_pattern=".*\\.7z$")

# Loop through each and create a parquet file
for file_name in file_list:
    print("ETL: " + file_name)

    # Decompress file
    file_path = proj.Config.paths.get("data_interim").joinpath(file_name)
    decomp_file(file_path=file_path,
                extract_path=proj.Config.paths.get("data_interim"),
                print_msg=True)

    # Convert decompressed csv to parquet file with custom schema
    file_path_csv = Path(sub("\\.7z$", "", str(file_path)))
    parquet_file_name = Path(sub("\\.csv\\.7z$", ".parquet", str(file_name)))
    parquet_file_path = proj.Config.paths.get("data_proc").joinpath(parquet_file_name)
    schema = get_schema(file_name)
    csv_to_parquet(read_path=file_path_csv,
                   write_path=parquet_file_path,
                   schema=schema)

# Clean up interim
clear_interm_data_dir()

