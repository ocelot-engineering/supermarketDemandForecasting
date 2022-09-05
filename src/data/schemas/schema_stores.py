from pyspark.sql.types import StructType, IntegerType, StringType

"""
+---------+-----+---------+----+-------+
|store_nbr| city|    state|type|cluster|
+---------+-----+---------+----+-------+
|        1|Quito|Pichincha|   D|     13|
|        2|Quito|Pichincha|   D|     13|
|        3|Quito|Pichincha|   D|      8|
+---------+-----+---------+----+-------+
"""

schema = StructType()\
    .add('store_nbr', IntegerType(), True)\
    .add('city', StringType(), True)\
    .add('state', StringType(), True)\
    .add('type', StringType(), True)\
    .add('cluster', StringType(), True) # is an integer, but given it's a grouping better to keep it categorical
