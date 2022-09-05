from pyspark.sql.types import StructType, IntegerType, StringType, DateType, FloatType, BooleanType

"""
+----------+----------+
|      date|dcoilwtico|
+----------+----------+
|2013-01-01|      null|
|2013-01-02|     93.14|
|2013-01-03|     92.97|
+----------+----------+
"""

schema = StructType()\
    .add('date', DateType(), True)\
    .add('dcoilwtico', FloatType(), True)
