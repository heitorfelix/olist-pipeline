# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT 'pedidos',COUNT(*) from silver.olist.pedidos
# MAGIC UNION ALL
# MAGIC SELECT 'itens_pedido', COUNT(*) from silver.olist.itens_pedido
# MAGIC UNION ALL
# MAGIC SELECT 'pagamento_pedido',COUNT(*) from silver.olist.pagamento_pedido

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE bronze.olist.products

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) from silver.olist.pagamento_pedido

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   product_id                           AS idProduto,
# MAGIC   product_category_name                AS descCategoriaProduto,
# MAGIC   product_name_lenght                  AS comprimentoNomeProduto,
# MAGIC   product_description_lenght           AS comprimentoDescricaoProduto,
# MAGIC   product_photos_qty                   AS quantidadeFotosProduto,
# MAGIC   product_weight_g                     AS pesoProduto,
# MAGIC   product_length_cm                    AS comprimentoProduto,
# MAGIC   product_height_cm                    AS alturaProduto,
# MAGIC   product_width_cm                     AS larguraProduto
# MAGIC FROM bronze.olist.products
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.olist.products

# COMMAND ----------

schema_name = 'olist'
table_name = 'sellers'
full_load_path = f"/Volumes/raw/{schema_name}/full/{table_name}/"
df = (spark
        .read
        .format('parquet')
        #.schema(.data_schema)
        .load(full_load_path))
schema = df.schema.json()


import json
with open(f'{table_name}.json', 'w') as f:
    f.write(schema)

# COMMAND ----------


