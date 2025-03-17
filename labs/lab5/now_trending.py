# now_trending.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, from_json
from pyspark.sql.functions import desc
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, count, when

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

# 4) Filter only "play", "skip", or "like" events
events_df = events_df.filter(col("action").isin("play", "skip", "like"))

# 5) Group by region + 1-minute processing time window
from pyspark.sql.functions import current_timestamp

windowed_df = events_df \
    .groupBy(
        window(current_timestamp(), "1 minutes"),  # processing-time window
        col("region"),
        col("song_id")
    ) \
    .agg(
        count(when(col("action") == "play", 1)).alias("play_count"),
        count(when(col("action") == "skip", 1)).alias("skip_count"),
        count(when(col("action") == "like", 1)).alias("like_count")
    )

# 6) Compute skip ratio and rank the songs based on play count
windowed_df = windowed_df.withColumn(
    "skip_ratio", 
    col("skip_count") / (col("play_count") + col("skip_count") + col("like_count"))
)

# 7) Rank by play_count and skip_ratio
def process_batch(batch_df, batch_id):
    """
    This function is called for each micro-batch. We rank by play_count and skip_ratio within each region & window.
    We'll show the top N songs, including skip ratio.
    """
    if batch_df.rdd.isEmpty():
        print("No data in this batch.")
        return

    # Window partition by region + the 'window' column
    w = Window.partitionBy("region", "window").orderBy(desc("play_count"), desc("skip_ratio"))

    ranked_df = batch_df.withColumn("rn", row_number().over(w)) \
                        .filter(col("rn") <= 3)

    # Show the top songs for each region + 1-minute window
    print(f"=== Batch: {batch_id} ===")
    ranked_df.show(truncate=False)

# 8) Write Stream with foreachBatch
query = windowed_df \
    .writeStream \
    .outputMode("update") \
    .trigger(processingTime='1 seconds') \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()

