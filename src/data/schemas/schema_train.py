from pyspark.sql.types import StructType, IntegerType, StringType, DateType, FloatType, BooleanType

schema = StructType()\
    .add('date', DateType(), True)\
    .add('store_nbr', IntegerType(), True)\
    .add('item_nbr', IntegerType(), True)\
    .add('unit_sales', FloatType(), True)\
    .add('onpromotion', BooleanType(), True)
