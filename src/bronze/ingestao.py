# Databricks notebook source
import delta

# COMMAND ----------

# DBTITLE 1,IMPORTS
schema_name = 'olist'
table_name = 'orders'

df_full = spark.read.format("parquet").load(f"/Volumes/raw/{schema_name}/full/{table_name}/")

(df_full.coalesce(1)
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("bronze.olist.orders"))

# COMMAND ----------

(spark.read
      .format("parquet")
      .load(f"/Volumes/raw/{schema_name}/cdc/{table_name}/")
      .createOrReplaceTempView(table_name))

# COMMAND ----------

bronze = delta.DeltaTable

# COMMAND ----------

query = """ 
    SELECT * 
    FROM orders
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY (SELECT 1) DESC) = 1
"""

df_cdc_unique = spark.sql(query)
df_cdc_unique.display()

# COMMAND ----------

bronze = delta.DeltaTable.forName(spark, f"bronze.olist.{table_name}")

# COMMAND ----------

#upsert

(bronze.alias("b")
       .merge(df_cdc_unique.alias("d"),
       "b.order_id = d.order_id")
       .whenMatchedDelete(condition= "d.Op = 'D'")
       .whenMatchedUpdateAll(condition= "d.Op = 'U'")
       .whenNotMatchedInsertAll(condition= "d.Op = 'I' or d.Op = 'U'")
       .execute()
)


# COMMAND ----------


