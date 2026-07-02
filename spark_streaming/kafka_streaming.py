from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.types import * 
import os
from dotenv import load_dotenv
load_dotenv()

## create the spark session 
spark = SparkSession.builder \
    .appName("KafkaCryptoConsumer2") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


## define the schema

coin_schema = StructType([
    StructField("id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("current_price", DoubleType(), True),
    StructField("market_cap", LongType(), True),
    StructField("total_volume", DoubleType(), True),
    StructField("high_24h", DoubleType(), True),
    StructField("low_24h", DoubleType(), True),
    StructField("last_updated", StringType(), True),
])

## define the schema for fullmessage

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("data", ArrayType(coin_schema), True)
])

os.environ.get("KAFKA_TOPIC","")
## read the data from kafka 
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", os.environ.get("KAFKA_TOPIC","")) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()


## convert the kafka raw value from byte to string 

json_df=raw_df.selectExpr("CAST(VALUE AS STRING) as json")

## now apply the schema and read the data.

parsed_df= json_df.select(from_json("json",schema=schema).alias("parsed_coin"))

flattened_df = parsed_df.select(
    col("parsed_coin.timestamp").alias("timestamp"),
    explode(col("parsed_coin.data")).alias("coin")
).select(
    "timestamp",
    col("coin.id").alias("id"),
    col("coin.symbol").alias("symbol"),
    col("coin.current_price").alias("price"),
    col("coin.market_cap"),
    col("coin.total_volume"),
    col("coin.high_24h"),
    col("coin.low_24h"),
    col("coin.last_updated")
)

# Define the output path for the Parquet files
output_path = os.environ.get("OUTPUT_PATH","")
checkpoint_path = os.environ.get("CHECKPOINT_PATH","")

# Write to Parquet files in the specified folder
query = flattened_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .start()

query.awaitTermination()