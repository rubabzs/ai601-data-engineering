# now_trending.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, from_json, count, when, desc, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


# 1) Create SparkSession
spark = SparkSession.builder \
    .appName("NowTrendingSongs") \
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
    StructField("timestamp", DoubleType(), True),  # or we can store as Double
    StructField("region", StringType(), True),
    StructField("action", StringType(), True)
])

json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data"))
events_df = parsed_df.select("data.*")

# Convert timestamp double -> actual timestamp if we want event time
# But for simplicity, let's do a processing-time approach
# If you want event-time windows, do:
# events_df = events_df.withColumn("event_time", (col("timestamp") * 1000).cast(TimestampType()))

# 4) Filter only "play" events
# plays_df = events_df.filter(col("action") == "play")

# 5) Group by region + 5-minute processing time window
# We'll do a simple processing-time window using current_timestamp
# Alternatively, you can do event-time with a column if you convert 'timestamp' to a Spark timestamp
from pyspark.sql.functions import current_timestamp

windowed_df = events_df.groupBy(
    window(col("timestamp").cast("timestamp"), "5 minutes"),
    col("region"),
    col("song_id")
).agg(
    count(when(col("action") == "play", True)).alias("play_count"),
    count(when(col("action") == "skip", True)).alias("skip_count"),
    count(when(col("action") == "like", True)).alias("like_count")
)


# 5) Compute skip ratio
windowed_df = windowed_df.withColumn(
    "skip_ratio",
    round(col("skip_count") / (col("play_count") + col("skip_count")), 2)
)

# 6) Use foreachBatch to do rank-based top N logic each micro-batch
def process_batch(batch_df, batch_id):
    """
    This function is called for each micro-batch. We treat 'batch_df' as a normal batch DataFrame.
    We'll rank by 'count' within each region & window and pick top 3 (or 5, or 100).
    """
    if batch_df.rdd.isEmpty():
        print("No data in this batch.")
        return

    # We'll partition by region + the 'window' column
    w = Window.partitionBy("region", "window").orderBy(desc("play_count"))
    print(batch_df.columns)
    ranked_df = batch_df.withColumn("rn", row_number().over(w))

    # unpopular_songs = batch_df.filter(col("skip_ratio") > 0.5)  # Songs with high skip ratio

    # Show the top songs for each region + 5-min window
    print(f"=== Batch: {batch_id} ===")
    print("Trending Songs:")
    ranked_df.show(truncate=False)

    # print("Unpopular Songs (Skip Ratio > 50%):")
    # unpopular_songs.show(truncate=False)

# 7) Write Stream with foreachBatch
query = windowed_df \
    .writeStream \
    .outputMode("update") \
    .foreachBatch(process_batch) \
    .trigger(processingTime='5 seconds') \
    .start()

query.awaitTermination()