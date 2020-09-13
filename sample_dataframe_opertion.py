from pyspark.shell import spark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import  *
from pyspark.context import *
from pyspark.sql.session import *
from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit
from pyspark.sql.dataframe import DataFrame

from dependencies.spark import start_spark
schema = StructType([
    StructField("person.name", StringType(), True),
    StructField("person", StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)]))
])

data = [
    ("charles", Row("chuck", 42)),
    ("lawrence", Row("larry", 73))
]
# spark = SparkSession.builder \
# .master("local") \
# .appName("Word Count") \
# .config("spark.some.config.option", "some-value") \
# .getOrCreate()

# s1 = SparkSession.builder.config("k1", "v1").getOrCreate()
# df = s1.createDataFrame(data, schema)
# df.show()
# df = sqlContext.createDataFrame(data, ["features"])
df = spark.createDataFrame(
    [("china", "asia"), ("colombia", "south america")],
    ["country.name", "continent"]
)
df.show()