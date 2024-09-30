import json
from pyspark.sql import types
import datetime

def import_schema(table_name: str):
    """
    Imports a schema from a JSON file for use in a Spark DataFrame.

    This function reads a schema definition from a JSON file, deserializes it, 
    and converts it into a Spark `StructType` schema.

    Args:
        table_name (str): The name of the table (and JSON file) to import the schema from. 
                          The function expects the JSON file to be in the current directory.

    Returns:
        StructType: A Spark `StructType` schema object that defines the structure of the DataFrame.
    """
    with open(f"{table_name}.json", "r") as open_file:
        schema_json = json.load(open_file) 

    schema_df = types.StructType.fromJson(schema_json)
    return schema_df


def table_exists(spark, catalog: str, database: str, table: str) -> bool:
    """
    Checks whether a table exists in a specified catalog and database in Spark.

    This function queries the Spark catalog to determine if a specific table exists 
    within a given catalog and database.

    Args:
        spark: The Spark session object.
        catalog (str): The catalog in which to look for the table.
        database (str): The database within the catalog where the table might exist.
        table (str): The name of the table to check for existence.

    Returns:
        bool: `True` if the table exists, `False` otherwise.
    """
    count = (spark.sql(f"SHOW TABLES FROM {catalog}.{database}")
                .filter(f"database = '{database}' AND tableName = '{table}'")
                .count())
    return count == 1

def import_query(table_name: str) -> str:
    """
    This function reads a SQL file and returns the query as a string.

    :param table_name: string containing the name of the SQL file to be read.

    :return: string containing the SQL query.
    """

    with open(f"{table_name}.sql", "r") as f:
        return f.read()

def create_merge_condition(id_field: str, left_alias: str, right_alias: str) -> str:
    """
    Return a merge condition based on one or more ID fields.
    
    :param id_field: string containing the name of the ID field separated by comma, ex: "order_id, order_item".

    :return: merge codition to be used against the DeltaTable.
    """
    # Divide a string em uma lista de campos de ID
    id_fields = [field.strip() for field in id_field.split(",")]
    
    # Cria a condição de comparação para cada par de colunas, uma por tabela (esquerda e direita)
    conditions = [f"{left_alias}.{field} = {right_alias}.{field}" for field in id_fields]
    
    # Junta todas as condições com " AND "
    merge_condition = " AND ".join(conditions)
    
    return merge_condition

def extract_from(query: str) -> str:
    """
    Extracts the table name from a SQL query.

    This function takes a SQL query in string format and extracts the table 
    name that appears after the "FROM" clause.

    Args:
        query (str): The SQL query from which the table name will be extracted.

    Returns:
        str: The name of the table used in the "FROM" clause of the query.
    """
    table_name = (query.lower()
                     .split("from")[-1]
                     .strip(" ")
                     .split(" ")[0]
                     .split("\n")[0]
                     .strip(" "))
    return table_name


def add_generic_from(query: str, generic_from: str = 'df') -> str:
    """
    Replaces the table name in a SQL query with a generic placeholder.

    This function modifies a SQL query by replacing the table name in the 
    "FROM" clause with a generic placeholder (default is 'df').

    Args:
        query (str): The original SQL query.
        generic_from (str, optional): The placeholder to replace the table name. 
                                      Defaults to 'df'.

    Returns:
        str: The modified query with the table name replaced by the generic placeholder.
    """
    table_name = extract_from(query)
    query = query.replace(table_name, generic_from)
    return query


def add_fields(query: str, fields: list) -> str:
    """
    Adds additional fields to the SELECT statement of a SQL query.

    This function appends new fields to the SELECT clause of a SQL query. 
    The new fields are inserted before the "FROM" clause.

    Args:
        query (str): The original SQL query.
        fields (list): A list of new field names to add to the SELECT statement.

    Returns:
        str: The modified query with the additional fields in the SELECT clause.
    """
    from_split = query.lower().split("from")
    select = from_split[0].strip(" \n")
    fields = ",\n".join(fields)
    from_query = f"\n\nFROM{from_split[-1]}"
    query_new = f"{select},\n{fields}{from_query}"
    return query_new


def format_query_cdf(query: str, from_table: str) -> str:
    """
    Formats a SQL query to include additional fields and a generic table name.

    This function modifies the query to include a list of change-data-capture (CDC) 
    fields and replaces the original table name with a provided table name.

    Args:
        query (str): The original SQL query.
        from_table (str): The generic table name to replace the original table name in the "FROM" clause.

    Returns:
        str: The modified query with additional CDC fields and the updated table name.
    """
    fields = ["_change_type", "_commit_version", "_commit_timestamp"]
    query = add_fields(query=query, fields=fields)
    query = add_generic_from(query=query, generic_from=from_table)
    return query

def date_range(start, stop):
    dt_start = datetime.datetime.strptime(start, "%Y-%m-%d")
    dt_stop = datetime.datetime.strptime(stop, "%Y-%m-%d")
    dates = []
    while dt_start < dt_stop:
        dates.append(dt_start.strftime("%Y-%m-%d"))
        dt_start += datetime.timedelta(days=1)
    return dates
