from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, from_json, current_timestamp, desc, sum as _sum, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# 1) Create SparkSession
spark = SparkSession.builder \
    .appName("NowTrendingSongsWithSkipRate") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2) Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "music_events") \
    .option("startingOffsets", "latest") \
    .load()

# 3) Parse the JSON 'value' from Kafka
schema = StructType([
    StructField("song_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("region", StringType(), True),
    StructField("action", StringType(), True)
])

json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data"))
events_df = parsed_df.select("data.*")

# Casting the timestamp
events_df = events_df.withColumn(
    "event_time",
    (col("timestamp") * 1000).cast(TimestampType())  # Seconds -> milliseconds and cast to TimestampType
)

# 4) Filter for "play" and "skip" actions
filtered_df = events_df.filter((col("action") == "play") | (col("action") == "skip"))

# Add a watermark of 1 minute on the event_time column
watermarked_df = filtered_df.withWatermark("event_time", "1 minutes")

# 5) Group by region + 1-minute processing time window and calculate play/skip counts
windowed_df = watermarked_df.groupBy(
    window(current_timestamp(), "1 minutes"),
    col("region"),
    col("song_id")
).agg(
    _sum(when(col("action") == "play", 1).otherwise(0)).alias("play_count"),
    _sum(when(col("action") == "skip", 1).otherwise(0)).alias("skip_count")
)

# Calculate skip rate
skip_rate_df = windowed_df.withColumn(
    "skip_rate",
    col("skip_count") / (col("play_count") + col("skip_count"))
)

# 6) Use foreachBatch to rank songs and include skip rate
def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print("No data in this batch.")
        return

    # Rank by play_count within each region + window
    w = Window.partitionBy("region", "window").orderBy(desc("play_count"))

    ranked_df = batch_df.withColumn("rn", row_number().over(w)) \
                        .filter(col("rn") <= 3)  # Top 3 songs per region

    # Show the top songs with skip rate
    print(f"=== Batch: {batch_id} ===")
    ranked_df.select("window", "region", "song_id", "play_count", "skip_count", "skip_rate").show(truncate=False)

# 7) Write Stream with foreachBatch
query = skip_rate_df.writeStream \
    .outputMode("update") \
    .trigger(processingTime='2 seconds') \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()