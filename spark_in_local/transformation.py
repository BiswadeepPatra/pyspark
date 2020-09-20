from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Window
#JAVA_HOME = "C:\Program Files\Java\jdk1.8.0_131"
if __name__=="__main__":
    print("Inside main")

    spark= SparkSession.builder.appName("prog1").master("local[*]").getOrCreate()
    lst_1=[(1,"sales",4200),(1,"admin",3100),(3,"sales",4000),(4,"sales",4000),(5,"admin",2700),(6,"dev",3400),(7,"dev",5200),(8,"dev",3700),(9,"dev",4400),(10,"dev",4400)]
    lst_2 = [(10, "sales", 4200), (10, "admin", 3100), (30, "sales", 4000), (40, "sales", 4000), (50, "admin", 2700),(6, "dev", 3400), (7, "dev", 5200), (8, "dev", 3700), (9, "dev", 4400), (10, "dev", 4400)]
    df_1=spark.createDataFrame(lst_1,["id","dept","salary"])
    df_2= spark.createDataFrame(lst_2, ["id", "dept", "salary"])
    # df_1.show()
    # df_2.show()
    # print(df_1.union(df_2).count())
    # print(df_1.or(df_2).count())
    df_or=(df_1.join(df_2, (df_1.id == df_2.id) | (df_1.salary == df_2.salary), how='inner'))
    # #print(df_1.join(df_2, (df_1.id == df_2.id) | (df_1.salary == df_2.salary) , how='inner').explain())
    # #df_1.join(df_2, df_1.id == df_2.id, how='inner')

    DFF1= df_1.join(df_2, df_1.salary == df_2.salary, how='inner')
    DFF2=df_1.join(df_2,df_1.id == df_2.id,how='inner')
    df_union = DFF1.union(DFF2)
    # #print((df_1.join(df_2, df_1.salary == df_2.salary, how='inner')).union(df_1.join(df_2,df_1.id == df_2.id,how='inner')).explain())
    print("with OR-----------")
    df_or.orderBy(df_1.id).show()

    print("with union")
    df_union.orderBy(df_1.id).show()
    print("DIFFFFFFF______________________")
    #df_union.union(df_union).except(df1.intersect(df_or)).show()
    df_union.exceptAll(df_or).show()