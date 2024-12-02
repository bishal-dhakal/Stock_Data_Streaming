from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("StockProcessing") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()
    # .config("spark.driver.host", "127.0.1.1") \

def Consumer_Spark():
    # Check if Spark is working
    df = spark.range(10).toDF("number")
    df.show()
