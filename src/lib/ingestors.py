import delta
from utils import import_schema, create_merge_condition, import_query, extract_from, format_query_cdf, date_range, table_exists
import tqdm

class Ingestor:

    def __init__(self, spark, catalog, schema_name, table_name, data_format):
        self.spark = spark 
        self.catalog = catalog
        self.schema_name = schema_name
        self.table_name = table_name
        self.format = data_format
        self.set_schema()
        self.checkpoint_location = f"/Volumes/raw/{self.schema_name}/cdc/{self.table_name}_checkpoint/"

    def set_schema(self):
        self.data_schema = import_schema(self.table_name)

    def load(self, path):
        df = (self.spark
                  .read
                  .format(self.format)
                  .schema(self.data_schema)
                  .load(path))
        return df
        
    def save(self, df):
        (df.write
           .format("delta")
           .mode("overwrite")
           .saveAsTable(f"{self.catalog}.{self.schema_name}.{self.table_name}"))
        return True
        
    def execute(self, path):
        df = self.load(path)
        return self.save(df)
    
class IngestorCDC(Ingestor):

    def __init__(self, spark, catalog, schema_name, table_name, data_format, id_field, timestamp_field):
        super().__init__(spark, catalog, schema_name, table_name, data_format)
        self.id_field = id_field
        self.timestamp_field = timestamp_field
        self.set_deltatable()

    def set_deltatable(self):
        table_name = f"{self.catalog}.{self.schema_name}.{self.table_name}"
        self.deltatable = delta.DeltaTable.forName(self.spark, table_name)

    def upsert(self, df):
        df.createOrReplaceGlobalTempView(f"view_{self.table_name}")
        query = f'''
            SELECT *
            FROM global_temp.view_{self.table_name}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY {self.id_field} ORDER BY {self.timestamp_field} DESC) = 1
        '''

        merge_condition = create_merge_condition(id_field = self.id_field, left_alias ='b', right_alias = 'd')

        df_cdc = self.spark.sql(query)

        (self.deltatable
             .alias("b")
             .merge(df_cdc.alias("d"), merge_condition) 
             .whenMatchedDelete(condition = "d.OP = 'D'")
             .whenMatchedUpdateAll(condition = "d.OP = 'U'")
             .whenNotMatchedInsertAll(condition = "d.OP = 'I' OR d.OP = 'U'")
             .execute())

    def load(self, path):
        df = (self.spark
                  .readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", self.format)
                  .schema(self.data_schema)
                  .load(path))
        return df
    
    def save(self, df):
        stream = (df.writeStream
                   .option("checkpointLocation", self.checkpoint_location)
                   .foreachBatch(lambda df, batchID: self.upsert(df))
                   .trigger(availableNow=True))
        return stream.start()
    

class IngestorCDF(IngestorCDC):

    def __init__(self, spark, catalog, schema_name, table_name, id_field, id_field_from):
        
        super().__init__(spark=spark,
                         catalog=catalog,
                         schema_name=schema_name,
                         table_name=table_name,
                         data_format='delta',
                         id_field=id_field,
                         timestamp_field='_commit_timestamp')
        
        self.id_field_from = id_field_from
        self.set_query()
        self.checkpoint_location = f"/Volumes/raw/{schema_name}/cdc/{catalog}_{table_name}_checkpoint/"

    def set_schema(self):
        return

    def set_query(self):
        query = import_query(self.table_name)
        self.from_table = extract_from(query=query)
        self.original_query = query
        self.query = format_query_cdf(query, "{df}")

    def load(self):
        df = (self.spark.readStream
                   .format('delta')
                   .option("readChangeFeed", "true")
                   .table(self.from_table))
        return df
    
    def save(self, df):
        stream = (df.writeStream
                    .option("checkpointLocation", self.checkpoint_location)
                    .foreachBatch(lambda df, batchID: self.upsert(df) )
                    .trigger(availableNow=True))
        return stream.start()
    
    def upsert(self, df):
        df.createOrReplaceGlobalTempView(f"silver_{self.table_name}")

        query_last = f"""
        SELECT *
        FROM global_temp.silver_{self.table_name}
        WHERE _change_type <> 'update_preimage'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY {self.id_field_from} ORDER BY _commit_timestamp DESC) = 1
        """
        df_last = self.spark.sql(query_last)
        df_upsert = self.spark.sql(self.query, df=df_last)

        merge_condition = create_merge_condition(id_field = self.id_field, left_alias ='s', right_alias = 'd')

        (self.deltatable
             .alias("s")
             .merge(df_upsert.alias("d"), merge_condition) 
             .whenMatchedDelete(condition = "d._change_type = 'delete'")
             .whenMatchedUpdateAll(condition = "d._change_type = 'update_postimage'")
             .whenNotMatchedInsertAll(condition = "d._change_type = 'insert' OR d._change_type = 'update_postimage'")
               .execute())

    def execute(self):
        df = self.load()
        return self.save(df)
    


class IngestorCubo:

    def __init__(self, spark, catalog, schema_name, table_name):
        self.spark = spark
        self.catalog = catalog
        self.schema_name = schema_name
        self.table_name = table_name
        self.table = f'{catalog}.{schema_name}.{table_name}'
        self.set_query()

    def set_query(self):
        self.query = import_query(self.table_name)

    def load(self, **kwargs):
        df = self.spark.sql(self.query.format(**kwargs))
        return df

    def save(self, df, dt_ref):
        self.spark.sql(f"DELETE FROM {self.table} WHERE dtRef = '{dt_ref}'")

    def backfill(self, dt_start, dt_stop):
        dates = date_range(dt_start, dt_stop)

        if not table_exists(self.spark, self.catalog, self.schema_name, self.table_name):
            df = self.load(dt_ref = dates.pop(0))
            df.write.saveAsTable(self.table)

            for dt in tqdm(dates):
                df = self.load(dt_ref=dt)
                self.save(df=df, dt_ref=dt)


    