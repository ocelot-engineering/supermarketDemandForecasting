from pyspark.sql.types import StructType, IntegerType, StringType, DateType, FloatType, BooleanType

"""
Note: removing ID since it is not useful.
+---+----------+---------+--------+----------+-----------+
| id|      date|store_nbr|item_nbr|unit_sales|onpromotion|
+---+----------+---------+--------+----------+-----------+
|  0|2013-01-01|       25|  103665|       7.0|       null|
|  1|2013-01-01|       25|  105574|       1.0|       null|
|  2|2013-01-01|       25|  105575|       2.0|       null|
+---+----------+---------+--------+----------+-----------+
"""

schema = StructType()\
    .add('date', DateType(), True)\
    .add('store_nbr', IntegerType(), True)\
    .add('item_nbr', IntegerType(), True)\
    .add('unit_sales', FloatType(), True)\
    .add('onpromotion', BooleanType(), True)
