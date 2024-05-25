# Databricks notebook source
#/FileStore/tables/LoanStats_2018Q4-2.csv

# COMMAND ----------

#file location and type
file_location="/FileStore/tables/LoanStats_2018Q4-1.csv"
fle_type="csv"

#CSV options
infer_schema="true"
first_row_is_header="true"
delimiter=","

df = spark.read.format("csv") \
  .option("multiline","true") \
  .option("quote","\"") \
  .option("escape","\"") \
  .option("inferSchema",infer_schema) \
  .option("header",first_row_is_header) \
  .option("sep",delimiter) \
  .load(file_location)
display(df)


# COMMAND ----------



# COMMAND ----------

df.count()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

temp_table_name = "Loanstats"
df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from loanstats

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)from loanstats

# COMMAND ----------

df.describe().show()
#for summary of the numerical coloumns.

# COMMAND ----------

# selecitng from with in dataframe that are the key or deciding factors
df_sel = df.select("term","home_ownership","grade","purpose","int_rate","addr_state","loan_status","application_type","loan_amnt","emp_length","annual_inc","dti","delinq_2yrs","revol_util","total_acc","num_tl_90g_dpd_24m","dti_joint")

# COMMAND ----------

df_sel.describe().show()

# COMMAND ----------

df_sel.describe("term","loan_amnt","emp_length","annual_inc","dti","delinq_2yrs","revol_util","total_acc").show()

# COMMAND ----------

#load this  into executable memory seperating from the local data set so it can execute fast
df_sel.cache()

# COMMAND ----------

df_sel.describe("loan_amnt","emp_length","dti","delinq_2yrs","revol_util","total_acc").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct emp_length from loanstats limit 50

# COMMAND ----------

# MAGIC %md
# MAGIC from the result we can see the null values why because they are being treated as the string so we ahve to make them numeric there are two ways

# COMMAND ----------

from pyspark.sql.functions import regexp_replace,regexp_extract
from pyspark.sql.functions import col

regex_string='years|year|\\+|\\<'
df_sel.select(regexp_replace(col("emp_length"),regex_string,"").alias("emplength_cleaned"),col("emp_length")).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC the regex is to extract the string adn repalce is to replace the string with "". telling which string or coloumn to hit or target.

# COMMAND ----------

#otehr method 
regex_String="\\d+"
df_sel.select(regexp_extract(col("emp_length"), regex_string,0).alias("emplnght_cleaned"),col("emp_length")).show(10)

# COMMAND ----------

df_sel.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC now we can see the df_Sel still shows the garbage value despite our cleaning. we ahve to updadte the df_sel we were just implementing in a cell.

# COMMAND ----------

df_sel= df_sel.withColumn("term_cleaned",regexp_replace(col("term"),"months","")).withColoumn("emplen_cleaned",regexp_extract(col("emp_length"),"\\d+",0))

# COMMAND ----------

import pyspark.sql.functions as F

df_sel = df_sel.withColumn("term_cleaned", F.regexp_replace(F.col("term"), "months", "")) \
               .withColumn("emplen_cleaned", F.regexp_extract(F.col("emp_length"), "\d+", 0))


# COMMAND ----------

# MAGIC %md
# MAGIC withColoumn is to create a new coloumn in a data frame.

# COMMAND ----------

df_sel.select('term','term_cleaned','emp_length','emplen_cleaned').show(15)

# COMMAND ----------

df_sel.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC AS we can see the the  schema is jsut like earlier now we have to update the schema

# COMMAND ----------

table_name="loanstatus_sel"
df_sel.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC we have created the new table to filter out more data

# COMMAND ----------

temp_table_name = "loanstatus_sel"
df_sel.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM loanstatus_sel

# COMMAND ----------

# MAGIC %md
# MAGIC Runnig the corelation amtrix

# COMMAND ----------

df_sel.stat.cov('annual_inc','loan_amnt')

# COMMAND ----------

# MAGIC %md
# MAGIC In corelation matrix is the number is close to the positive 1 this shows that are corelated . if the numbr is towards the -1 less corelation

# COMMAND ----------

df_sel.stat.corr('annual_inc','loan_amnt')

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Also Runing is SQL is same 

# COMMAND ----------

# MAGIC %sql
# MAGIC select corr(loan_amnt,term_cleaned) from loanstatus_sel

# COMMAND ----------

# MAGIC %md
# MAGIC using crosstab function
# MAGIC crosstab('loan_status', 'grade'): This part of the code calls the crosstab function, providing two arguments:
# MAGIC **'loan_status': The first argument specifies the column you want to use as the rows in the contingency table. In this case, it's the loan_status of the loans.
# MAGIC **'grade': The second argument specifies the column you want to use as the columns in the contingency table. Here, it's the grade of the loan

# COMMAND ----------

df_sel.stat.crosstab('loan_status','grade').show()

# COMMAND ----------

freq=df_sel.stat.freqItems(['purpose','grade'],0.3)

# COMMAND ----------

freq.collect()

# COMMAND ----------

df_sel.groupby('purpose').count().show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC To see the top catorgories by using the desc() function

# COMMAND ----------

df_sel.groupby('purpose').count().orderBy(col('count').desc()).show()

# COMMAND ----------

from pyspark.sql.functions import count,mean,stddev_pop,min,max,avg

# COMMAND ----------

# MAGIC %md
# MAGIC Aggregation Functions

# COMMAND ----------

quantileProbs = [0.25,0.5,0.75,0.9]
relError= 0.05
df_sel.stat.approxQuantile("loan_amnt",quantileProbs,relError)

# COMMAND ----------

# MAGIC %md
# MAGIC hen the error is close to 1 which means we are getting

# COMMAND ----------

quantileProbs = [0.25,0.5,0.75,0.9]
relError= 0.0
df_sel.stat.approxQuantile("loan_amnt",quantileProbs,relError)

# COMMAND ----------

from pyspark.sql.functions import isnan, when, count, col

# Assuming df_sel is your DataFrame
df_sel.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_sel.columns]).show()


# COMMAND ----------

# MAGIC %sql
# MAGIC select loan_status, count(*) from loanstats group by loan_status

# COMMAND ----------

df_sel=df_sel.na.drop("all",subset=["loan_status"])

# COMMAND ----------

df_sel.select([count(when(isnan(c) | col(c).isNull(),c)).alias(c) for c in df_sel.columns]).show

# COMMAND ----------

df_sel.count()

# COMMAND ----------

df_sel.describe("dti","revol_util").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can see that there are nULL values and we have to clean the table now we will clean the data using SQL 

# COMMAND ----------

# MAGIC %sql
# MAGIC select ceil(REGEXP_REPLACE(revol_util,"\%","" )),COUNT(*) from loanstatus_sel group by ceil(REGEXP_REPLACE(revol_util,"\%",""))

# COMMAND ----------

df_sel=df_sel.withColumn("revolutil_cleaned",regexp_extract(col("revol_util"),"\\d+",0))

# COMMAND ----------

df_sel.describe('revol_util','revolutil_cleaned').show()

# COMMAND ----------

def fill_avg(df, colname):
    return df.select(colname).agg(avg(colname))

# COMMAND ----------

rev_avg=fill_avg(df_sel,'revolutil_cleaned')

# COMMAND ----------

# MAGIC %md
# MAGIC now I got the average just have to assign it back to the data frame

# COMMAND ----------

from pyspark.sql.functions import lit
rev_avg=fill_avg(df_sel,'rvolutil_cleaned').first()[0]
df_sel=df_sel.withColumn('rev_avg',lit(rev_avg))

# COMMAND ----------

# MAGIC %md
# MAGIC the coalesce function will take the all the value from the table whenever it is null it will take the rev_avg coloumn we have created

# COMMAND ----------

from pyspark.sql.functions import coalesce
df_sel=df_sel.withColumn('revolutil_cleaned',coalesce(col('revolutil_cleaned'),col('rev_avg')))

# COMMAND ----------

df_sel.describe('revol_util','revolutil_cleaned').show()

# COMMAND ----------

# MAGIC %md
# MAGIC taking the coloumn and casting as the double to show its actual vlaue

# COMMAND ----------

df_sel=df_sel.withColumn("revolutil_cleaned",df_sel["revolutil_cleaned"].cast("double"))

# COMMAND ----------

df_sel.describe('revol_util','revolutil_cleaned').show()

# COMMAND ----------

df_sel.select([count(when(isnan(c) | col(c).isNull(),c)).alias(c) for c in df_sel.columns]).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from loanstatus_sel where dti is null

# COMMAND ----------

# MAGIC %md
# MAGIC looking at the table and knowing the dataset we can say whenever the dti is  null there is joint applicant

# COMMAND ----------

# MAGIC %sql
# MAGIC select application_type,dti,dti_joint from loanstats where dti is null

# COMMAND ----------

df_sel=df_sel.withColumn("dti_cleaned",coalesce(col("dti"),col("dti_joint")))

# COMMAND ----------

# MAGIC %md
# MAGIC where ever there is null vlaue the colasce funciton will take the  value of dti_joint

# COMMAND ----------

df_sel.select([count(when(isnan(c) | col(c).isNull(),c)).alias(c) for c in df_sel.columns]).show()

# COMMAND ----------

df_sel.groupby('loan_status').count().show()

# COMMAND ----------

df_sel.where(df_sel.loan_status.isin(["Late (31-120 days)","Charged Off","In Grace Period","Late (16-30 days)"])).show()

# COMMAND ----------

df_sel=df_sel.withColumn("bad_loan",when(df_sel.loan_status.isin(["Late(31-120 days)","Charged Off","In Grace Period","Late (19-39 days)"]),'Yes').otherwise('No'))

# COMMAND ----------

df_sel.groupBy('bad_loan').count().show()

# COMMAND ----------

df_sel.filter(df_sel.bad_loan =='Yes').show()

# COMMAND ----------

df_sel.printSchema()

# COMMAND ----------

df_sel_final= df_sel.drop('revol_util','dti','dti_joint')

# COMMAND ----------

df_sel_final.printSchema()

# COMMAND ----------

df_sel.stat.crosstab('bad_loan','grade').show()

# COMMAND ----------

df_sel.describe('dti_cleaned').show()

# COMMAND ----------

df_sel.filter(df_sel.dti_cleaned > 100).show()

# COMMAND ----------

permanent_table_name = "lc_loan_data"
df_sel.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lc_loan_data
