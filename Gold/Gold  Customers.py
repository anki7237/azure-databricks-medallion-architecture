# Databricks notebook source
# MAGIC %md
# MAGIC ### **Data Reading From Source**

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id
import pyspark.sql.types
from pyspark.sql.functions import monotonically_increasing_id, lit
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# COMMAND ----------

df=spark.sql("select *  from databricks_catalog.silver.customers_silver")

# COMMAND ----------

dbutils.widgets.text("init_load_flag","1")

# COMMAND ----------

init_load_flag=int(dbutils.widgets.get("init_load_flag"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Remove Duplicates**

# COMMAND ----------

df=df.dropDuplicates(["customer_id"])
df.display()

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, lit
df=df.withColumn("DimCustomersKey",monotonically_increasing_id()+lit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC **Dividing New Vs Old Records**

# COMMAND ----------

if init_load_flag == 0:
    df_old=spark.sql('''select DimCustomersKey,customer_id,create_date,update_date from databricks_catalog.gold.DimCustomers''')

else:
    df_old=spark.sql('''select 0 DimCustomersKey, 0 customer_id,0 create_date,0 update_date from databricks_catalog.silver.customers_silver where 1=0''')
   

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Renaming Columns of df_old**

# COMMAND ----------

df_old=df_old.withColumnRenamed("DimCustomersKey","old_DimCustomersKey").withColumnRenamed("customer_id","old_customer_id").withColumnRenamed("create_date","old_create_date").withColumnRenamed("update_date","old_update_date")

# COMMAND ----------

# MAGIC %md
# MAGIC **Applying the join with Old Records**
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col
df_join=df.join(df_old, col('customer_id') == col('old_customer_id'),"left")


# COMMAND ----------

df_join.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Seperating New Vs Old Value**

# COMMAND ----------

df_new=df_join.filter((df_join.old_DimCustomersKey.isNull()))

# COMMAND ----------

df_old=df_join.filter((df_join.old_DimCustomersKey.isNotNull()))

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Preparing df_old**

# COMMAND ----------

#Dropping all the columns which are not required
df_old=df_old.drop('old_customer_id','old_update_date','old_DimCustomersKey')

#Renaming Old DimCustomersKey column to DimCustomersKey
#df_old=df_old.withColumnRenamed("old_DimCustomersKey#","DimCustomersKey")

#Renaming old_create_date colum to create_date
df_old=df_old.withColumnRenamed("old_create_date","create_date")
df_old=df_old.withColumn("create_date",to_timestamp(col("create_date")))

#Recreating "update_date" column with current timestamp
from pyspark.sql.functions import current_timestamp
# Recreating "update_date" column with current timestamp
df_old=df_old.withColumn("update_date", current_timestamp())


# COMMAND ----------

df_old.limit(10).display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **Preparing DF NEW**

# COMMAND ----------

df_new=df_new.drop("old_DimCustomersKey","old_customer_id","old_create_date","old_update_date")

#Recreating "update_date" and create_date column with current timestamp

df_new=df_new.withColumn("update_date", current_timestamp())
df_new=df_new.withColumn("create_date", current_timestamp())




# COMMAND ----------

df_new.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Surrogate Key -From 1**

# COMMAND ----------

df_new=df_new.withColumn("DimCustomersKey",monotonically_increasing_id()+lit(1))



# COMMAND ----------

# MAGIC %md
# MAGIC **Adding max Surrogate Key**

# COMMAND ----------

if init_load_flag==1:
    max_surrogate_key=0
else:
    df_max_surr=spark.sql('''select max(DimCustomersKey) as max_surrogate_key from databricks_catalog.gold.DimCustomersKey''')
    max_surrogate_key=df_max_surr.collect()[0]['max_surrogate_key']

# COMMAND ----------

df_new=df_new.withColumn("DimCustomersKey",lit(max_surrogate_key)+col("DimCustomersKey"))


# COMMAND ----------

# MAGIC %md
# MAGIC **Union of df_old and df_new**

# COMMAND ----------

final_data=df_new.unionByName(df_old)

# COMMAND ----------

final_data.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE databricks_catalog.gold.DimCustomers;

# COMMAND ----------

final_data.write.mode("overwrite").format("delta").option("path","abfss://gold@databricksite.dfs.core.windows.net/DimCustomers")\
.saveAsTable("databricks_catalog.gold.DimCustomers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **SCD Type 1**

# COMMAND ----------

if spark.catalog.tableExists("databricks_catalog.gold.DimCustomers"):
     dlt_obj=DeltaTable.forPath(spark,"abfss://gold@databricksite.dfs.core.windows.net/DimCustomers")
     dlt_obj.alias("dlt").merge(final_data.alias("new"), "dlt.DimCustomersKey = new.DimCustomersKey").whenMatchedUpdateAll()\
     .whenNotMatchedInsertAll().execute()
    
else:
    final_data.write.mode("overwrite").format("delta").option("path","abfss://gold@databricksite.dfs.core.windows.net/DimCustomers")\
    .saveAsTable("databricks_catalog.gold.DimCustomers")
     
