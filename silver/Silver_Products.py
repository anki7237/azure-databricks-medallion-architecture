# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import expr

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df=spark.read.format("parquet").load("abfss://bronze@databricksite.dfs.core.windows.net/products")


# COMMAND ----------


df=df.drop(col("_rescued_data"))

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Functions**

# COMMAND ----------

# DBTITLE 1,Cell 7
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION databricks_catalog.bronze.discount_func(p_price DOUBLE) 
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE PYTHON
# MAGIC AS
# MAGIC $$
# MAGIC     return p_price * 0.90
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC Select product_id,price,databricks_catalog.bronze.discount_func(price) from products

# COMMAND ----------

df=df.withColumn("discounted_price",expr("databricks_catalog.bronze.discount_func(price)"))
df.display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION databricks_catalog.bronze.upper_func(p_brand STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC AS $$
# MAGIC    return p_brand.upper()
# MAGIC $$;
# MAGIC select databricks_catalog.bronze.upper_func(brand) from products   

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("path","abfss://silver@databricksite.dfs.core.windows.net/products").save()

# COMMAND ----------

df=spark.read.format("delta").load("abfss://silver@databricksite.dfs.core.windows.net/products")
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catalog.silver.products_silver
# MAGIC USING DELTA
# MAGIC LOCATION
# MAGIC 'abfss://silver@databricksite.dfs.core.windows.net/products';