# Databricks notebook source
# DBTITLE 1,Imports
import delta
import json
from pyspark.sql import types
from utils import table_exists, import_schema




# COMMAND ----------

# DBTITLE 1,Setup
schema_name = 'olist'
catalog = 'bronze'


# table_name = 'orders'
# id_column = 'order_id'
# timestamp_column = 'modified_at'

table_name = dbutils.widgets.get('table_name')
id_column =  dbutils.widgets.get('id_column')
timestamp_column =  dbutils.widgets.get('timestamp_column')

schema = import_schema(table_name=table_name)



# COMMAND ----------

# DBTITLE 1,Full Load
if table_exists(spark, catalog, schema_name, table_name):
        print(f'Tabela {table_name} já existe. Pulando carga Full.')

else:
        print(f'Tabela  {table_name} não existe. Fazendo carga Full...')

        df_full = (spark.read
                        .format("parquet")
                        .schema(schema)
                        .load(f"/Volumes/raw/{schema_name}/full/{table_name}/"))
        
        
        (df_full.coalesce(1)
                .write
                .format("delta")
                .mode("overwrite")
                .saveAsTable(f"bronze.{schema_name}.{table_name}"))

# COMMAND ----------

# DBTITLE 1,CDC


# Creating temp view to load cdc
(spark.read
      .format("parquet")
      .schema(schema)
      .load(f"/Volumes/raw/{schema_name}/cdc/{table_name}/")
      .createOrReplaceTempView(table_name))

# Get last value
query = f""" 
SELECT * 
FROM {table_name}
QUALIFY ROW_NUMBER() OVER (PARTITION BY {id_column} ORDER BY {timestamp_column} DESC) = 1
"""

df_cdc_last = spark.sql(query)


#upsert
bronze = delta.DeltaTable.forName(spark, f"bronze.{schema_name}.{table_name}")

(bronze.alias("b")
       .merge(df_cdc_last.alias("d"),
       "b.order_id = d.order_id")
       .whenMatchedDelete(condition= "d.Op = 'D'")
       .whenMatchedUpdateAll(condition= "d.Op = 'U'")
       .whenNotMatchedInsertAll(condition= "d.Op = 'I' or d.Op = 'U'")
       .execute()
)

