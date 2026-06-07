# Databricks notebook source
# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time

# COMMAND ----------

df=spark.read.format("parquet").load("abfss://bronze@databricksite.dfs.core.windows.net/customers")

# COMMAND ----------

df.display()

# COMMAND ----------

df=df.drop("_rescued_data")

# COMMAND ----------

df=df.withColumn("domain_name",split(col("email"), "@")[1])
df.display()


# COMMAND ----------

df.groupBy("domain_name").agg(count(col("customer_id")).alias("total_customers")).sort("total_customers",ascending=False).display()

# COMMAND ----------

df_gmail=df.filter(col('domain_name').like("%gmail.com%"))
df_gmail.display()
time.sleep(5)

df_yahoo=df.filter(col('domain_name').like("%yahoo.com%"))
df_yahoo.display()
time.sleep(5)

df_hotmail=df.filter(col('domain_name').like("%hotmail.com%"))
df_hotmail.display()
time.sleep(5)


# COMMAND ----------

df=df.withColumn("full_name",concat(col("first_name"),lit("-"),col("last_name")))
df=df.drop(col("first_name"),col("last_anme"))

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format("delta").mode("append").save("abfss://silver@databricksite.dfs.core.windows.net/customers")

# COMMAND ----------

df=spark.read.format("delta").load("abfss://silver@databricksite.dfs.core.windows.net/customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS databricks_catalog.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catalog.silver.customers_silver
# MAGIC USING DELTA
# MAGIC LOCATION
# MAGIC 'abfss://silver@databricksite.dfs.core.windows.net/customers' 

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from databricks_catalog.silver.customers_silver