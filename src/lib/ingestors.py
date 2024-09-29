import delta
from utils import import_schema, create_merge_condition


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

        merge_condition = create_merge_condition(id_field = self.id_field)

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
    