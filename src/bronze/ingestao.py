# Databricks notebook source
# DBTITLE 1,Imports
import delta
import json
from pyspark.sql import types
import sys

sys.path.insert(0, "../lib/")


from utils import table_exists, import_schema
from ingestors import Ingestor, IngestorCDC


# COMMAND ----------

# DBTITLE 1,Setup
schema_name = 'olist'
catalog = 'bronze'

table_name = dbutils.widgets.get('table_name')
id_column =  dbutils.widgets.get('id_column')
timestamp_column =  dbutils.widgets.get('timestamp_column')

# table_name = 'products'
# id_column = 'product_id'
# timestamp_column = 'modified_at'

cdc_path = f"/Volumes/raw/{schema_name}/cdc/{table_name}/"
full_load_path = f"/Volumes/raw/{schema_name}/full/{table_name}/"
checkpoint_location = f"/Volumes/raw/{schema_name}/checkpoint/{table_name}/"
schema = import_schema(table_name=table_name)



# COMMAND ----------

# DBTITLE 1,Full load
if table_exists(spark, catalog, schema_name, table_name):
        print(f'Tabela {table_name} já existe. Pulando carga Full.')

else:
        
        ingestor = Ingestor(spark
                               , catalog = catalog
                               , schema_name = schema_name
                               , table_name = table_name
                               , data_format = 'parquet') 
        
        dbutils.fs.rm(ingestor.checkpoint_location, True)
        print(f'Tabela  {table_name} não existe. Fazendo carga Full...')
        ingestor.execute(path=full_load_path)


# COMMAND ----------

# DBTITLE 1,CDC Stream
ingestor = IngestorCDC(spark
                        , catalog = catalog
                        , schema_name = schema_name
                        , table_name = table_name
                        , data_format = 'parquet'
                        , id_field = id_column
                        , timestamp_field = timestamp_column) 

ingestor.execute(cdc_path)
