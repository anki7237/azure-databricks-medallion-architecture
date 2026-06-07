# Databricks notebook source
# MAGIC %md
# MAGIC ### Fact Orders

# COMMAND ----------

df=spark.sql('''select * from databricks_catalog.silver.orders_silver''')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_catalog.gold.dimproducts

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating joining dfs off customer and product**

# COMMAND ----------

df_dimcus=spark.sql('''select DimCustomersKey,customer_id as dim_customer_id from databricks_catalog.gold.dimcustomers''')
df_dimpro=spark.sql('''select product_id as DimProductsKey,product_id  as dim_product_id from databricks_catalog.gold.dimproducts''')


# COMMAND ----------

# MAGIC %md
# MAGIC **Creating a fact table**

# COMMAND ----------

df_fact=df.join(df_dimcus,df['customer_id']== df_dimcus['dim_customer_id'],how='left').join(df_dimpro,df['product_id']== df_dimpro['dim_product_id'],how='left')
df_fact_new=df_fact.drop("dim_customer_id","dim_product_id","customer_id","product_id")


# COMMAND ----------

# MAGIC %md
# MAGIC **Upsert on Fact Table**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("databricks_catalog.gold.FactOrders"):
    dlt_obj=DeltaTable.forName(spark,"databricks_catalog.gold.FactOrders")
    dlt_obj.alias("t").merge(df_fact.alias("s"),"t.order_id=s.order_id and t.DimCustomersKey=s.DimCustomersKey and t.DimProductsKey=s.DimProductsKey").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df_fact_new.write.format("delta").option("path","abfss://gold@databricksite.dfs.core.windows.net/FactOrders").saveAsTable("databricks_catalog.gold.FactOrders")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_catalog.gold.Factorders