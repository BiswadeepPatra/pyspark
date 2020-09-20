from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Window
#JAVA_HOME = "C:\Program Files\Java\jdk1.8.0_131"
if __name__=="__main__":
    print("prog1 is running")

    spark= SparkSession.builder.appName("prog1").master("local[*]").getOrCreate()
    #input_file="file:///C://Users//Biswadeep//Downloads//Q4_2018.csv"
    #input_rdd=spark.sparkContext.textFile(input_file)
    #df=spark.read.csv(input_file,header='true')
   # print(df.show(100))
   # df.createOrReplaceTempView("my_table")
    #spark.sql("select * from my_table ").show(10)
    lst=[(1,"sales",4200),(1,"admin",3100),(3,"sales",4000),(4,"sales",4000),(5,"admin",2700),(6,"dev",3400),(7,"dev",5200),(8,"dev",3700),(9,"dev",4400),(10,"dev",4400)]
    df=spark.createDataFrame(lst,["id","dept","salary"])
    df.show()
    df_new=df.groupBy("dept").agg(F.expr("avg(salary)").alias("average_salary"),F.expr("sum(salary)").alias("total_salary"))
    df_new.show()
    window_spec=Window.partitionBy("dept").orderBy(F.desc("salary"))
    df_wind=df.withColumn("average_sal",F.avg(F.col("salary")).over(window_spec)).withColumn("total_salary",F.sum(F.col("salary")).over(window_spec))
    df_wind.show()