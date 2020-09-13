# Using withColumnRenamed

def changeColName(df):
    for col in df.columns :
      #print(col)
      new_col="%s_1" % col
      #print(new_col)
      df=df.withColumnRenamed(col,new_col)
    return df
df3=changeColName(df)
df3.columns

#