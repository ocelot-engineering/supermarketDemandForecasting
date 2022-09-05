from pyspark.sql.types import StructType, IntegerType, StringType, DateType, FloatType, BooleanType

"""
+----------+---------+------------+
|      date|store_nbr|transactions|
+----------+---------+------------+
|2013-01-01|       25|         770|
|2013-01-02|        1|        2111|
|2013-01-02|        2|        2358|
+----------+---------+------------+
"""

schema = StructType()\
    .add('date', DateType(), True)\
    .add('store_nbr', IntegerType(), True)\
    .add('transactions', IntegerType(), True)
