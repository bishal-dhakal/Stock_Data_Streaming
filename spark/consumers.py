from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)
from pyspark.sql.functions import from_json, col, from_unixtime, date_format
from utils.constants import TOPIC
import redis
import json
import signal
import sys


class ConsumerSpark:
    def __init__(self):
        self.spark = (
            SparkSession.builder.appName("KafkaRealTimeProcessing")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1",
            )
            .config("spark.sql.shuffle.partitions", "5")
            .getOrCreate()
        )

        self.spark.conf.set(
            "spark.sql.streaming.forceDeleteTempCheckpointLocation", "true"
        )
        self.schema = StructType(
            [
                StructField("ticker", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("volume", IntegerType(), True),
                StructField("created_at", DoubleType(), True),
            ]
        )

        # redis client
        self.redis_pool = redis.ConnectionPool(host="localhost", port=6379, db=0)
        self.redis_client = redis.StrictRedis(connection_pool=self.redis_pool)

    def read_stream(self):
        raw_data = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", TOPIC)
            .option("kafka.group.id", "consumer_spark")
            .option("startingOffsets", "latest")
            .option("maxOffsetsPerTrigger", 1000)
            .load()
        )

        parsed_data = raw_data.select(
            from_json(col("value").cast("string"), self.schema).alias("data")
        )

        parsed_data = parsed_data.withColumn(
            "created_at", from_unixtime(col("data.created_at")).cast("timestamp")
        )

        parsed_data = parsed_data.select(
            "data.ticker",
            "data.price",
            "data.volume",
            date_format("created_at", "yyyy-MM-dd HH:mm:ss").alias("created_at"),
        )
        return parsed_data

    def save_to_redis(self, batch_df, batch_id):
        pipeline = self.redis_client.pipeline()
        for row in batch_df.collect():
            stream_key = f"stock:{row['ticker']}"
            
            # Construct the data for the stream
            message = {
                "price": row["price"],
                "volume": row["volume"],
                "created_at": row["created_at"]
            }

            # Convert the data to JSON string (as Redis stream values are typically stored as strings)
            value_json = json.dumps(message)

            # XADD command to insert data into the Redis stream
            pipeline.xadd(stream_key, {
                "price": row["price"],
                "volume": row["volume"],
                "created_at": row["created_at"]
            })
            pipeline.expire(stream_key, 86400)  # Optional: set the stream to expire after 24 hours
            print(f"Saved to Redis Stream: {stream_key} --> {message}")

        # Execute the pipeline to batch the commands
        pipeline.execute()

    def start_streaming(self):
        parsed_data = self.read_stream()

        parsed_data.printSchema()

        query = (
            parsed_data.writeStream.outputMode("append")
            .foreachBatch(self.save_to_redis)
            .start()
        )

        query.awaitTermination()

def main():
    consumer = ConsumerSpark()
    while True:
        consumer.start_streaming()


if __name__ == "__main__":
    main()
