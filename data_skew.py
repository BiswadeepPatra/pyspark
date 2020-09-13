#explode the small DF and multiple random to big DF
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col

big=[1,2,3,4,5,6,7,8,9,8,7,6,5,4,3]

rdd1 = sc.parallelize(small)
row_rdd = rdd1.map(lambda x: Row(x))
Small=sqlContext.createDataFrame(row_rdd,['numbers_1'])

small=[1,2,3,4]
rdd12 = sc.parallelize(big)
row_rdd2 = rdd12.map(lambda x: Row(x))
Big=sqlContext.createDataFrame(row_rdd2,['numbers_2'])

Small=Small.withColumn('rand_col', (functions.rand()*5).cast(IntegerType()))
Small=Small.withColumn('KeyS',functions.concat(col("numbers_1"),col("rand_col")))

Big=Big.withColumn('expld',functions.explode(functions.array(lit(0),lit(1),lit(2),lit(3),lit(4))))
Big=Big.withColumn('KeyB',functions.concat(col("numbers_2"),col("expld")))

Big.join(Small,Big.KeyB == Small.KeyS, how='left').show()


