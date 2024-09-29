# Databricks notebook source
def import_query(table_name: str) -> str:
    with open(f"./{table_name}.sql", "r") as f:
        return f.read()
    
schema_name = 'olist'
table_name = 'pagamento_pedido'

query = import_query(table_name)

(spark.sql(query)
      .write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(f"silver.{schema_name}.{table_name}"))
