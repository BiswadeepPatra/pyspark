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
    # df.show()
    # df_new=df.groupBy("dept").agg(F.expr("avg(salary)").alias("average_salary"),F.expr("sum(salary)").alias("total_salary"))
    # df_new.show()
    window_spec=Window.partitionBy("dept").orderBy(F.desc("salary"))
    # df_wind=df.withColumn("average_sal",F.avg(F.col("salary")).over(window_spec)).withColumn("total_salary",F.sum(F.col("salary")).over(window_spec))
    # df_wind.show()

    df.withColumn("denserank",F.dense_rank().over(window_spec))\
        .withColumn("rank",F.rank().over(window_spec))\
        .withColumn("denserank",F.dense_rank().over(window_spec)).show()
    #
    # +---+-----+------+
    # | id | dept | salary |
    # +---+-----+------+
    # | 1 | sales | 4200 |
    # | 1 | admin | 3100 |
    # | 3 | sales | 4000 |
    # | 4 | sales | 4000 |
    # | 5 | admin | 2700 |
    # | 6 | dev | 3400 |
    # | 7 | dev | 5200 |
    # | 8 | dev | 3700 |
    # | 9 | dev | 4400 |
    # | 10 | dev | 4400 |
    # +---+-----+------+
    # with group by
    # +-----+------------------+------------+
    # | dept | average_salary | total_salary |
    # +-----+------------------+------------+
    # | dev | 4220.0 | 21100 |
    # | sales | 4066.6666666666665 | 12200 |
    # | admin | 2900.0 | 5800 |
    # +-----+------------------+------------+
    # with window funtions
    # +---+-----+------+------------------+------------+
    # | id | dept | salary | average_sal | total_salary |
    # +---+-----+------+------------------+------------+
    # | 7 | dev | 5200 | 5200.0 | 5200 |
    # | 9 | dev | 4400 | 4666.666666666667 | 14000 |
    # | 10 | dev | 4400 | 4666.666666666667 | 14000 |
    # | 8 | dev | 3700 | 4425.0 | 17700 |
    # | 6 | dev | 3400 | 4220.0 | 21100 |
    # | 1 | sales | 4200 | 4200.0 | 4200 |
    # | 3 | sales | 4000 | 4066.6666666666665 | 12200 |
    # | 4 | sales | 4000 | 4066.6666666666665 | 12200 |
    # | 1 | admin | 3100 | 3100.0 | 3100 |
    # | 5 | admin | 2700 | 2900.0 | 5800 |
#     # +---+-----+------+------------------+------------+
# +---+-----+------+-------+
# | id| dept|salary|row_num|
# +---+-----+------+-------+
# |  7|  dev|  5200|      1|
# |  9|  dev|  4400|      2|
# | 10|  dev|  4400|      3|
# |  8|  dev|  3700|      4|
# |  6|  dev|  3400|      5|
# |  1|sales|  4200|      1|
# |  3|sales|  4000|      2|
# |  4|sales|  4000|      3|
# |  1|admin|  3100|      1|
# |  5|admin|  2700|      2|
# +---+-----+------+-------+
# +---+-----+------+----+
# | id| dept|salary|rank|
# +---+-----+------+----+
# |  7|  dev|  5200|   1|
# |  9|  dev|  4400|   2|
# | 10|  dev|  4400|   2|
# |  8|  dev|  3700|   4|
# |  6|  dev|  3400|   5|
# |  1|sales|  4200|   1|
# |  3|sales|  4000|   2|
# |  4|sales|  4000|   2|
# |  1|admin|  3100|   1|
# |  5|admin|  2700|   2|
# +---+-----+------+----+

# +---+-----+------+---------+
# | id| dept|salary|denserank|
# +---+-----+------+---------+
# |  7|  dev|  5200|        1|
# |  9|  dev|  4400|        2|
# | 10|  dev|  4400|        2|
# |  8|  dev|  3700|        3|
# |  6|  dev|  3400|        4|
# |  1|sales|  4200|        1|
# |  3|sales|  4000|        2|
# |  4|sales|  4000|        2|
# |  1|admin|  3100|        1|
# |  5|admin|  2700|        2|
# +---+-----+------+---------+