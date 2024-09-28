import json
from pyspark.sql import types
import datetime



def import_schema(table_name:str):
    with open(f"{table_name}.json", "r") as open_file:
        schema_json = json.load(open_file) 

    schema_df = types.StructType.fromJson(schema_json)
    return schema_df


def table_exists(spark, catalog, database, table):
    count = (spark.sql(f"SHOW TABLES FROM {catalog}.{database}")
                .filter(f"database = '{database}' AND tableName = '{table}'")
                .count())
    return count == 1

    