from pyspark.sql.types import StructType, IntegerType, StringType, DateType, FloatType, BooleanType

"""
+---------+----------+---------+--------+-----------+
|       id|      date|store_nbr|item_nbr|onpromotion|
+---------+----------+---------+--------+-----------+
|125497040|2017-08-16|        1|   96995|      False|
|125497041|2017-08-16|        1|   99197|      False|
|125497042|2017-08-16|        1|  103501|      False|
+---------+----------+---------+--------+-----------+
"""

schema = StructType() \
    .add('id', IntegerType(), True) \
    .add('date', DateType(), True)\
    .add('store_nbr', IntegerType(), True)\
    .add('item_nbr', IntegerType(), True)\
    .add('onpromotion', BooleanType(), True)
