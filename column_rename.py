# Link   ----   https://mungingdata.com/pyspark/rename-multiple-columns-todf-withcolumnrenamed/

##########################################################################
Using withColumnRenamed
#########################################################################

def changeColName(df):
    for col in df.columns :
      #print(col)
      new_col="%s_1" % col
      #print(new_col)
      df=df.withColumnRenamed(col,new_col)
    return df
df3=changeColName(df)
df3.columns

##############################################################################
Using quinn & with_columns_renamed
##############################################################################
import quinn
def spaces_to_underscores(s):
    return s.replace("_", "--")
actual_df = quinn.with_columns_renamed(spaces_to_underscores)(df3)
actual_df.show()

#####################################################################
toDF for renaming columns
######################################################################

df3.toDF(*(c.replace(' ', '_') for c in df3.columns))
#####################################################################
Renaming some columns from a map

The with_some_columns_renamed function takes two arguments:

The first argument is a function specifies how the strings should be modified
The second argument is a function that returns True if the string should be modified and False otherwise
######################################################################

import quinn
mapping = {"id": "new_id", "name": "new_name","salaryww": "new_salary"}
def british_to_american(s):
    return mapping[s]
def change_col_name(s):
    return s in mapping
actual_df = quinn.with_some_columns_renamed(british_to_american, change_col_name)(df2)
actual_df.show()

################################
