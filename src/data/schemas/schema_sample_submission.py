from pyspark.sql.types import StructType, IntegerType, StringType, DateType, FloatType, BooleanType

"""
Note: this is not input data, but instead an illustration of the structure of a submission to Kaggle.
Notice ID is included here, meaning we must continue to keep ID for the test data.
+---------+----------+
|       id|unit_sales|
+---------+----------+
|125497040|         0|
|125497041|         0|
|125497042|         0|
+---------+----------+
"""

schema = StructType()\
    .add('id', IntegerType(), True)\
    .add('unit_sales', FloatType(), True)
