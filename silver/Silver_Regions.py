# Databricks notebook source
df=spark.read.table("databricks_cata.bronze.regions")

# COMMAND ----------

df=df.drop("_rescued_data")

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@databricksite.dfs.core.windows.net/regions")

# COMMAND ----------

df.display()

# COMMAND ----------

df=spark.read.format("delta").load("abfss://silver@databricksite.dfs.core.windows.net/regions")
df.display()


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catalog.silver.regions_silver
# MAGIC USING DELTA
# MAGIC LOCATION
# MAGIC 'abfss://silver@databricksite.dfs.core.windows.net/regions'