from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Step 1: Initialize SparkSession
spark = SparkSession.builder \
    .appName("KafkaConsumerApp") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

# Step 2: Kafka Configurations
kafka_brokers = "localhost:9092"
kafka_topic = "test-stream-data"

# Step 3: Read Data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Step 4: Decode Kafka Messages
decoded_df = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .withColumnRenamed("key", "message_key") \
    .withColumnRenamed("value", "message_value")

# Step 5: Print Kafka Messages to Console
query = decoded_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Await termination to keep the application running
query.awaitTermination()