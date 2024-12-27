import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_json

class Transformer:
    def __init__(self):
        self.spark = SparkSession.builder.appName("DataTransformer").getOrCreate()
        self.logger = logging.getLogger("Transformer")

    def transform(self, messages):
        try:
            rdd = self.spark.sparkContext.parallelize([json.dumps(msg) for msg in messages])
            df = self.spark.read.json(rdd)
            transformed_df = df.select(
                col("body.type").alias("Event"),
                col("body.object.uri").alias("AccountId"),
                col("messageId").alias("SEQUENCENUMBER"),
                current_timestamp().alias("CreatedDateTime"),
                current_timestamp().alias("UpdatedDateTime"),
                to_json(col("body")).alias("Json_Payload")
            )
            return transformed_df
        except Exception as e:
            self.logger.error(f"Transformation error: {e}")
            return None