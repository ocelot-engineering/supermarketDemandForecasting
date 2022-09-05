from pyspark.sql.types import StructType, IntegerType, StringType, DateType, FloatType, BooleanType

"""
+--------+---------+-----+----------+
|item_nbr|   family|class|perishable|
+--------+---------+-----+----------+
|   96995|GROCERY I| 1093|         0|
|   99197|GROCERY I| 1067|         0|
|  103501| CLEANING| 3008|         0|
+--------+---------+-----+----------+
"""

schema = StructType()\
    .add('item_nbr', IntegerType(), True)\
    .add('family', StringType(), True)\
    .add('class', StringType(), True)\
    .add('perishable', IntegerType(), True)
