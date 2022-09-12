#
# Functions used in ETL jobs
#

from typing import Union
import os
from pyspark.sql.types import StructType


# File decompression ----------------------------------------------

def decomp_file(file_path: Union[str, os.PathLike],
                extract_path: Union[str, os.PathLike],
                print_msg: bool = False) -> None:
    """
    Pass in a file path and decompress it to a location.
    :param file_path: file to be extracted
    :param extract_path: location to be extracted to
    :return: None
    """
    from pathlib import Path

    if print_msg:
        print("Decompressing: " + str(file_path) + " to " + str(extract_path))

    if not Path(file_path).exists():
        raise FileNotFoundError(file_path, " does not exist.")

    if Path(file_path).suffix == ".zip":
        decomp_zip(file_path, extract_path)
    elif Path(file_path).suffix == ".7z":
        decomp_7z(file_path, extract_path)
    else:
        raise NotImplementedError("Cannot handle ", Path(file_path).suffix, " file type.")

    return


def decomp_zip(file_path: Union[str, os.PathLike], extract_path: Union[str, os.PathLike]) -> None:
    import zipfile

    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)

    return None


def decomp_7z(file_path: Union[str, os.PathLike], extract_path: Union[str, os.PathLike]) -> None:
    import py7zr

    with py7zr.SevenZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)

    return None


# File reading and writing  ----------------------------------------------

def csv_to_parquet(read_path: Union[str, os.PathLike],
                   write_path: Union[str, os.PathLike],
                   schema: StructType) -> None:
    """
    Produce a parquet file from a csv.
    Note: this uses PySpark.
    :param read_path: csv file path to convert to parquet
    :param write_path: path of the resulting parquet file
    :param schema: schema for the file
    :return: None
    """
    from pathlib import Path
    from pyspark.sql import SparkSession

    if not Path(read_path).exists():
        raise FileNotFoundError(read_path, " does not exist.")
    if Path(write_path).exists():
        raise FileExistsError(write_path, " already exists.")

    if not Path(read_path).suffix == ".csv":
        raise OSError(read_path, " is not .csv")
    if not Path(write_path).suffix == ".parquet":
        raise OSError(write_path, " is not .parquet.")

    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv(str(read_path), header=True, schema=schema)
    df.write.parquet(str(write_path), mode='overwrite')

    return None


def get_file_list(dir_path: Union[str, os.PathLike], regex_etx_pattern: str = "(.*\\.csv$|.*\\.7z$)") -> list[str]:
    from os import listdir
    from re import compile
    file_list_all = listdir(dir_path)
    regex = compile(regex_etx_pattern)
    return list(filter(regex.match, file_list_all))


def clear_interm_data_dir() -> None:
    """
    This function is can only be used for clearing the interim data directory.
    Hence the path is hardcoded with the configured data interim path.
    """
    from config import proj
    from shutil import rmtree
    import re
    import os

    path_interim_data_dir = proj.Config.paths.get("data_interim")
    file_list = os.listdir(path_interim_data_dir)
    regex = re.compile("(.*\\.csv$|.*\\.7z$|.*\\.parquet$)")
    file_list_all = list(filter(regex.match, file_list))

    for file in file_list_all:
        file_path = path_interim_data_dir.joinpath(file)
        print("Deleting: " + str(file_path))
        if file_path.is_dir():
            rmtree(file_path)
        else:
            os.remove(file_path)

    print(str(path_interim_data_dir) + " cleared.")

