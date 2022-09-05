#
# Functions used in ETL jobs
#

# File decompression ----------------------------------------------

def decomp_file(file_path: str, extract_path: str):
    """
    Pass in a file path and decompress it to a location.
    :param file_path: file to be extracted
    :param extract_path: location to be extracted to
    :return: None
    """
    from pathlib import Path

    if not Path(file_path).exists():
        raise FileNotFoundError(file_path, " does not exist.")

    if Path(file_path).suffix == ".zip":
        decomp_zip(file_path, extract_path)
    elif Path(file_path).suffix == ".7z":
        decomp_7z(file_path, extract_path)
    else:
        raise NotImplementedError("Cannot handle ", Path(file_path).suffix, " file type.")

    return


def decomp_zip(file_path: str, extract_path: str):
    import zipfile

    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)

    return


def decomp_7z(file_path: str, extract_path: str):
    import py7zr

    with py7zr.SevenZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)

    return


# File reading and writing  ----------------------------------------------

def csv_to_parquet(read_path: str, write_path: str, schema):
    """
    Produce a parquet file from a csv.
    Note: this uses PySpark.
    :param read_path: csv file path to convert to parquet
    :param write_path: path of the resulting parquet file
    :param overwrite: overwrite the write path if already exists
    :return: None
    """
    from pathlib import Path
    from pyspark.sql import SparkSession

    if not Path(read_path).exists():
        raise FileNotFoundError(read_path, " does not exist.")
    if not Path(write_path).exists():
        raise FileExistsError(write_path, " already exists.")

    if not Path(read_path).suffix == ".csv":
        raise OSError(read_path, " is not .csv")
    if not Path(write_path).suffix == ".parquet":
        raise OSError(write_path, " is not .parquet.")

    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv(read_path, header=True, schema=schema)
    df.write.parquet(write_path, mode='overwrite')

    return


def get_schema(file_name: str):
    """
    Returns the schema object of a file path passed in.
    Schema must be saved under `src/data/schemas/` with the format schema_<file_name>.py.
    e.g. `src/data/schemas/schema_transactions.py`
    :param file_name: file name or file path e.g. `items.csv` or `items`
    :return: StructType
    """
    import os.path
    from re import sub
    from importlib import import_module

    file_name_w_etx = os.path.basename(file_name)
    file_name = sub("\\.py$", "", str(file_name_w_etx))

    schema_package = import_module("src.data.schemas.schema_" + file_name)

    return schema_package.schema

