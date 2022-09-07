"""
Script used to download data from Kaggle and save to project.
This will not do any ETL of the data.
See src/data/etl_main.py for next steps.
"""

from config import proj
import kaggle
from pathlib import Path

# See readme at https://github.com/Kaggle/kaggle-api
# Credentials already set

KAGGLE_COMP_NAME = 'favorita-grocery-sales-forecasting'
PATH_RAW_DATA = proj.Config.paths.get("data_raw")

if PATH_RAW_DATA.is_dir():
    kaggle.api.authenticate()
    kaggle.api.competition_download_files(competition=KAGGLE_COMP_NAME, path=PATH_RAW_DATA)
else:
    raise NotADirectoryError(str(PATH_RAW_DATA.resolve()) + ' does not exist.')

print("Download complete.")
