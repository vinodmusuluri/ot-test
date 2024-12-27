# from pyspark.sql import SparkSession
# from pyspark.sql.types import StringType
# from pyspark.sql.functions import col

# spark = SparkSession.builder \
#                 .appName("SnowflakeIntegration") \
#                 .config("spark.jars", "/Users/vinod.musuluri@onetrust.com/spark-jars/spark-sql_2.12-3.5.3.jar,/Users/vinod.musuluri@onetrust.com/spark-jars/spark-sql-kafka-0-10_2.12-3.5.3.jar,/Users/vinod.musuluri@onetrust.com/spark-jars/kafka-clients-3.5.2.jar") \
#                 .config("spark.sql.streaming.metricsEnabled", "true") \
#                 .config("spark.eventLog.enabled", "true") \
#                 .config("spark.eventLog.dir", "/Users/vinod.musuluri@onetrust.com/spark-jars") \
#                 .getOrCreate()

# spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
# jars = spark.sparkContext._conf.get("spark.jars")
# kafka_df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "test-stream-data") \
#     .option("startingOffsets", "earliest") \
#     .load()
# query = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").writeStream \
#     .format("console") \
#     .start()
# query.awaitTermination()


from kafka import KafkaConsumer

class read_topic:
    def __init__(self,kafka_server):
        self.kafka_server = kafka_server
      
    def readMessages(self, each_topic):
        consumer = KafkaConsumer(each_topic, bootstrap_servers = [self.kafka_server],
                                 auto_offset_reset = 'earliest', enable_auto_commit = True,
                                 value_deserializer = lambda x: (x.decode("utf-8")))
        for message in consumer : 
            print(message.key, message.value)    
    def connectKafka(self,topic_list):  
        print(len(topic_list))
        if len(topic_list) > 1: 
            print(topic_list)
            for each_topic in topic_list:
                print(each_topic)
                self.readMessages(each_topic)
        else :
            print("inside else")
            self.readMessages(topic_list[0])

ins = read_topic("localhost:9092")
ins.connectKafka(["test-stream-data","test-stream-data"])   
            




