from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col, from_unixtime, date_format
from utils.constants import TOPIC 

class ConsumerSpark:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("KafkaRealTimeProcessing") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1") \
            .config("spark.sql.shuffle.partitions", "5") \
            .getOrCreate()

        self.spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        self.schema = StructType([
            StructField("ticker", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("volume", IntegerType(), True),
            StructField("created_at", DoubleType(), True)
        ])

    def read_stream(self):
        raw_data = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", TOPIC) \
            .option("kafka.group.id", "consumer_spark") \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", 1000) \
            .load()

        parsed_data = raw_data.select(from_json(col("value").cast("string"), self.schema).alias("data"))

        parsed_data = parsed_data.withColumn(
            "created_at", from_unixtime(col("data.created_at")).cast("timestamp")
        )

        parsed_data = parsed_data.select(
            "data.ticker",
            "data.price",
            "data.volume",
            date_format("created_at", "yyyy-MM-dd HH:mm:ss").alias("created_at")
        )

        return parsed_data

    def start_streaming(self):
        parsed_data = self.read_stream()

        parsed_data.printSchema()

        query = parsed_data.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false")  \
            .start()

        query.awaitTermination()
