from pyspark.sql import SparkSession,functions as F
from pyspark.sql.types import StructType,StructField,StringType,LongType,IntegerType

if __name__=="__main__":
    spark=SparkSession.builder.getOrCreate()
    lst = [Row(1, "sales", 4200), Row(1, "admin", 3100), Row(3, "sales", 4000)]
    df_auto = spark.createDataFrame(lst, ["id", "dept", "salary"])
    #df_auto.show()
    #spark
    print(df_auto.printSchema())
    schema=StructType([ StructField(name="id",dataType=IntegerType(),nullable=True),
                        StructField(name="dept",dataType=StringType(),nullable=True),
                        StructField(name="salary",dataType=IntegerType(),nullable=False)
    ]
    )
    paarRows=spark.sparkContext.parallelize(lst)
    df_struct=spark.createDataFrame(paarRows,schema)
    #df_struct.show()
    print(df_struct.printSchema())