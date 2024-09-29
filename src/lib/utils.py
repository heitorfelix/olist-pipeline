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


def create_merge_condition(id_field: str) -> str:
    """
    Retorna uma condição de merge para múltiplas colunas de id.
    
    :param id_field: String contendo os nomes das colunas de id separadas por vírgula, ex: "order_id, order_item".

    :return: Condição de merge para ser usada no método merge do DeltaTable.
    """
    # Divide a string em uma lista de campos de ID
    id_fields = [field.strip() for field in id_field.split(",")]
    
    # Cria a condição de comparação para cada par de colunas, uma por tabela (esquerda e direita)
    conditions = [f"b.{field} = d.{field}" for field in id_fields]
    
    # Junta todas as condições com " AND "
    merge_condition = " AND ".join(conditions)
    
    return merge_condition