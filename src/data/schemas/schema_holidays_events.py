from pyspark.sql.types import StructType, IntegerType, StringType, DateType, FloatType, BooleanType

"""
+----------+-------+--------+-----------+--------------------+-----------+
|      date|   type|  locale|locale_name|         description|transferred|
+----------+-------+--------+-----------+--------------------+-----------+
|2012-03-02|Holiday|   Local|      Manta|  Fundacion de Manta|      False|
|2012-04-01|Holiday|Regional|   Cotopaxi|Provincializacion...|      False|
|2012-04-12|Holiday|   Local|     Cuenca| Fundacion de Cuenca|      False|
+----------+-------+--------+-----------+--------------------+-----------+
"""

schema = StructType()\
    .add('date', DateType(), True)\
    .add('type', StringType(), True)\
    .add('locale', StringType(), True)\
    .add('locale_name', StringType(), True)\
    .add('description', StringType(), True)\
    .add('transferred', BooleanType(), True)