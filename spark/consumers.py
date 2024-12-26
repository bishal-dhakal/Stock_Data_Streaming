from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col, from_unixtime, date_format
import redis
import json
import signal
import sys
import time
from datetime import datetime
from prometheus_client import Counter, Gauge, start_http_server

class ConsumerSpark:
    def __init__(self):
        # Metrics
        self.processed_messages = Counter('processed_messages', 'Messages processed')
        self.processing_errors = Counter('processing_errors', 'Processing errors')
        self.redis_latency = Gauge('redis_latency', 'Redis write latency')
        
        # Start Prometheus metrics server
        start_http_server(8000)
        
        self._initialize_spark()
        self._initialize_redis()
        self._setup_signal_handlers()
        
    def _initialize_spark(self):
        self.spark = (
            SparkSession.builder.appName("KafkaRealTimeProcessing")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1")
            .config("spark.sql.shuffle.partitions", "5")
            .getOrCreate()
        )
        self.spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        
        self.schema = StructType([
            StructField("ticker", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("volume", IntegerType(), True),
            StructField("created_at", DoubleType(), True),
        ])

    def _initialize_redis(self, max_retries=3):
        retry_count = 0
        while retry_count < max_retries:
            try:
                self.redis_pool = redis.ConnectionPool(
                    host="localhost", 
                    port=6379, 
                    db=0,
                    max_connections=10,
                    socket_timeout=5.0
                )
                self.redis_client = redis.StrictRedis(connection_pool=self.redis_pool)
                self.redis_client.ping()  # Test connection
                break
            except redis.ConnectionError as e:
                retry_count += 1
                if retry_count == max_retries:
                    raise Exception(f"Failed to connect to Redis after {max_retries} attempts")
                time.sleep(2 ** retry_count)  # Exponential backoff

    def _setup_signal_handlers(self):
        def signal_handler(signum, frame):
            print("Shutting down gracefully...")
            self.spark.stop()
            self.redis_pool.disconnect()
            sys.exit(0)
            
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def save_to_redis(self, batch_df, batch_id):
        start_time = time.time()
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                pipeline = self.redis_client.pipeline()
                
                for row in batch_df.collect():
                    stream_key = f"stock:{row['ticker']}"
                    message = {
                        "price": row["price"],
                        "volume": row["volume"],
                        "created_at": row["created_at"],
                        "processed_at": datetime.now().isoformat()
                    }
                    
                    pipeline.xadd(stream_key, message)
                    pipeline.expire(stream_key, 86400)
                    
                    # Update metrics
                    self.processed_messages.inc()
                
                pipeline.execute()
                
                # Record latency
                self.redis_latency.set(time.time() - start_time)
                break
                
            except redis.ConnectionError as e:
                retry_count += 1
                if retry_count == max_retries:
                    self.processing_errors.inc()
                    raise Exception(f"Failed to write batch {batch_id} after {max_retries} attempts")
                time.sleep(2 ** retry_count)
                self._initialize_redis()

    def read_stream(self):
        try:
            raw_data = (
                self.spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "stock_data")
                .option("kafka.group.id", "consumer_spark")
                .option("startingOffsets", "latest")
                .option("maxOffsetsPerTrigger", 1000)
                .option("failOnDataLoss", "false")
                .load()
            )

            parsed_data = (
                raw_data.select(from_json(col("value").cast("string"), self.schema).alias("data"))
                .withColumn("created_at", from_unixtime(col("data.created_at")).cast("timestamp"))
                .select(
                    "data.ticker",
                    "data.price",
                    "data.volume",
                    date_format("created_at", "yyyy-MM-dd HH:mm:ss").alias("created_at")
                )
            )
            
            return parsed_data
            
        except Exception as e:
            self.processing_errors.inc()
            raise Exception(f"Failed to read stream: {e}")

    def start_streaming(self):
        try:
            parsed_data = self.read_stream()
            
            query = (
                parsed_data.writeStream
                .outputMode("append")
                .foreachBatch(self.save_to_redis)
                .option("checkpointLocation", "/tmp/checkpoint")
                .trigger(processingTime="1 second")
                .start()
            )
            
            query.awaitTermination()
            
        except Exception as e:
            print(f"Streaming failed: {e}")
            self.processing_errors.inc()
            raise

if __name__ == "__main__":
    consumer = ConsumerSpark()
    consumer.start_streaming()