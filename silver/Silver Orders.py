# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

df=spark.read.format("parquet")\
.load("abfss://bronze@databricksite.dfs.core.windows.net/orders")

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
df=df.withColumnRenamed("_rescued_data", "rescued_data")


# COMMAND ----------

df.display()

# COMMAND ----------

df=df.drop("rescued_data")

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df=df.withColumn("order_date",to_timestamp(col("order_date")))

# COMMAND ----------

df.show(5)

# COMMAND ----------

df=df.withColumn("year",year(col("order_date")))

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df1=df.withColumn("flag",dense_rank().over(Window.partitionBy(col("year")).orderBy(col("total_amount").desc())))

# COMMAND ----------

df1.show(5)

# COMMAND ----------

df1=df1.withColumn("rankflag",rank().over(Window.partitionBy(col("year")).orderBy(col("total_amount").desc())))

# COMMAND ----------

df1.display()

# COMMAND ----------

df1=df1.withColumn("row_flag",row_number().over(Window.partitionBy(col("year")).orderBy(col("total_amount").desc())))
df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Classes --OOP**

# COMMAND ----------

class windows:
  def dense_rank(self,df):
    df_dense_rank=df.withColumn("flag",dense_rank().over(Window.partitionBy(col("year")).orderBy(col("total_amount").desc())))
    return df_dense_rank 
  def rank(self,df):
    df_rank=df.withColumn("rankflag",rank().over(Window.partitionBy(col("year")).orderBy(col("total_amount").desc())))
    return df_rank
  def row_number(self,df):
    df_row_number=df.withColumn("row_flag",row_number().over(Window.partitionBy(col("year")).orderBy(col("total_amount")
                                                                                                      .desc())))
    return df_row_number


# COMMAND ----------

df_new=df
df_new.display()

# COMMAND ----------

obj=windows()
df_result=obj.dense_rank(df_new)

# COMMAND ----------

df_result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

df.write.format("delta").mode("append").save("abfss://silver@databricksite.dfs.core.windows.net/orders")

# COMMAND ----------

df=spark.read.format("delta").load("abfss://silver@databricksite.dfs.core.windows.net/orders")
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catalog.silver.orders_silver
# MAGIC USING DELTA
# MAGIC LOCATION
# MAGIC 'abfss://silver@databricksite.dfs.core.windows.net/orders'