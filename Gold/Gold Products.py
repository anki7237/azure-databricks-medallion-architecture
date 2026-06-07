# Databricks notebook source
# MAGIC %md
# MAGIC ### DLT Pipelines

# COMMAND ----------

from pyspark import pipelines as dp 
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Streaming tables**

# COMMAND ----------

# MAGIC %md
# MAGIC ###Expectations

# COMMAND ----------


my_rules={
    "rule1" : "product_id is NOT NULL",
    "rule_2": "product_name IS NOT NULL"
}

# COMMAND ----------

@dp.table

@dp.expect_all_or_drop(my_rules)
def DimProducts_stage():
    df = spark.readStream.table("databricks_catalog.silver.products_silver")
    return df


               

# COMMAND ----------

@dp.view
def DimProducts_view():
    df = spark.readStream.table("DimProducts_stage")
    return df

# COMMAND ----------

@dp.table(name="DimProducts")
def DimProducts():
    return spark.read.table(
        "databricks_catalog.silver.products_silver"
    )

# COMMAND ----------

