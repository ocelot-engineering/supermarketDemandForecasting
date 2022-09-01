from pyspark.sql.types import StructType, IntegerType, StringType

schema = StructType()\
    .add('store_nbr', IntegerType(), True)\
    .add('city', StringType(), True)\
    .add('state', StringType(), True)\
    .add('type', StringType(), True)\
    .add('cluster', StringType(), True) # is an integer, but given it's a grouping better to keep it categorical
