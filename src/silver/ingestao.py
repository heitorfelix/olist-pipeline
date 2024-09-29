# Databricks notebook source

import sys

sys.path.insert(0, "../lib")

from utils import table_exists, extract_from, format_query_cdf, import_query
from ingestors import IngestorCDF

    
schema_name = 'olist'
catalog = 'silver'

table_name = dbutils.widgets.get('table_name')
idfield = dbutils.widgets.get("id_field")
idfield_old = dbutils.widgets.get("id_field_from")

# table_name = 'pagamento_pedido'
# id_field = 'idPedido' 
# id_field_from = 'order_id'

remove_checkpoint = False

if not table_exists(spark, catalog, schema_name, table_name):
    print(f'Table {catalog}.{schema_name}.{table_name} does not exists. Starting full load...')
    query = import_query(table_name)

    (spark.sql(query)
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.{schema_name}.{table_name}"))
    
    remove_checkpoint = True

else:
    print(f'Table {table_name} already exists. Starting CDF load...')


ingest = IngestorCDF(spark=spark,
                               catalog=catalog,
                               schema_name=schema_name,
                               table_name=table_name,
                               id_field=id_field,
                               id_field_from=id_field_from)

if remove_checkpoint:
    dbutils.fs.rm(ingest.checkpoint_location, True)

stream = ingest.execute()
print('Done')
