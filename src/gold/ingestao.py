# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

import tqdm
import sys
import datetime

sys.path.insert(0, "../lib")

from utils import *
from ingestors import IngestorCubo



# COMMAND ----------

today = (spark.sql("SELECT MAX(dtCompra) as max_dtCompra FROM silver.olist.pedidos").collect()[0]['max_dtCompra'])

catalog = "gold"
schema_name = 'olist'


try:
    table_name = dbutils.widgets.get("table_name")
    start = dbutils.widgets.get("dt_start") # now
    stop = dbutils.widgets.get("dt_stop") # now
except:
    start = '2018-06-01'
    stop =  '2018-12-01'
    table_name = 'daily_reports'

if start == today.strftime('%Y-%m-%d'):
    start = (today - datetime.timedelta(days=1)).strftime("%Y-%m-%d")



# COMMAND ----------

ingestor = IngestorCubo(spark=spark,
                                  catalog=catalog,
                                  schema_name=schema_name,
                                  table_name=table_name)

ingestor.backfill(start, stop)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.olist.daily_reports

# COMMAND ----------


