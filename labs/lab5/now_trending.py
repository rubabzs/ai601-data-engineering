# # now_trending.py

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, window, from_json
# from pyspark.sql.functions import desc
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType
# from pyspark.sql.types import TimestampType
# from pyspark.sql.window import Window
# from pyspark.sql.functions import row_number

# # 1) Create SparkSession
# spark = SparkSession.builder \
#     .appName("NowTrendingSongs") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # 2) Read from Kafka
# kafka_df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "music_events") \
#     .option("startingOffsets", "latest") \
#     .load()

# # 3) Parse the JSON 'value' from Kafka
# schema = StructType([
#     StructField("song_id", StringType(), True),
#     StructField("timestamp", DoubleType(), True),  # or we can store as Double
#     StructField("region", StringType(), True),
#     StructField("action", StringType(), True)
# ])

# json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

# parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data"))
# events_df = parsed_df.select("data.*")

# # Convert timestamp double -> actual timestamp if we want event time
# # But for simplicity, let's do a processing-time approach
# # If you want event-time windows, do:
# # events_df = events_df.withColumn("event_time", (col("timestamp") * 1000).cast(TimestampType()))

# # 4) Filter "play" and "skip" events
# plays_df = events_df.filter(col("action").isin("play", "skip"))

# # plays_df = events_df.filter(col("action") == "play")

# # 5) Group by region + 5-minute processing time window
# # We'll do a simple processing-time window using current_timestamp
# # Alternatively, you can do event-time with a column if you convert 'timestamp' to a Spark timestamp
# from pyspark.sql.functions import current_timestamp

# windowed_df = plays_df \
#     .groupBy(
#         window(current_timestamp(), "1 minutes"),  # processing-time window
#         col("region"),
#         col("song_id")
#     ) \
#     .count()

# # 6) Use foreachBatch to do rank-based top N logic each micro-batch
# def process_batch(batch_df, batch_id):
#     """
#     This function is called for each micro-batch. We treat 'batch_df' as a normal batch DataFrame.
#     We'll rank by 'count' within each region & window and pick top 3 (or 5, or 100).
#     """
#     if batch_df.rdd.isEmpty():
#         print("No data in this batch.")
#         return

#     # We'll partition by region + the 'window' column
#     w = Window.partitionBy("region", "window").orderBy(desc("count"))

#     ranked_df = batch_df.withColumn("rn", row_number().over(w)) \
#                         .filter(col("rn") <= 3)

#     # Show the top songs for each region + 5-min window
#     print(f"=== Batch: {batch_id} ===")
#     ranked_df.show(truncate=False)

# # 7) Write Stream with foreachBatch
# query = windowed_df \
#     .writeStream \
#     .outputMode("update") \
#     .foreachBatch(process_batch) \
#     .start()

# query.awaitTermination()

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, window, from_json, count, when, desc
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType
# from pyspark.sql.functions import current_timestamp

# # 1) Create SparkSession
# spark = SparkSession.builder \
#     .appName("NowTrendingSongs") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # 2) Read from Kafka
# kafka_df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "music_events") \
#     .option("startingOffsets", "latest") \
#     .load()

# # 3) Define Schema for Incoming Data
# schema = StructType([
#     StructField("song_id", StringType(), True),
#     StructField("timestamp", DoubleType(), True),
#     StructField("region", StringType(), True),
#     StructField("action", StringType(), True)
# ])

# # Parse the JSON 'value' from Kafka
# json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
# parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data"))
# events_df = parsed_df.select("data.*")

# # 4) Filter "play" and "skip" actions
# plays_and_skips_df = events_df.filter(col("action").isin("like", "skip"))

# # 5) Compute Play & Skip Counts with a 10-Minute Window
# skip_ratio_df = plays_and_skips_df \
#     .groupBy(
#         window(current_timestamp(), "10 minutes"),
#         col("region"),
#         col("song_id")
#     ) \
#     .agg(
#         count(when(col("action") == "like", True)).alias("play_count"),
#         count(when(col("action") == "skip", True)).alias("skip_count")
#     ) \
#     .withColumn("skip_ratio", col("skip_count") / (col("play_count") + col("skip_count")))

# # 6) Filter Top Skipped Songs (Skip Ratio > 50%)
# high_skip_songs_df = skip_ratio_df.filter(col("skip_ratio") > 0.5)

# # 7) Print Results in Real-Time
# def process_batch(batch_df, batch_id):
#     if batch_df.rdd.isEmpty():
#         print("No data in this batch.")
#         return
#     print(f"=== Batch {batch_id}: Top Skipped Songs ===")
#     batch_df.orderBy(desc("skip_ratio")).show(truncate=False)

# query = high_skip_songs_df \
#     .writeStream \
#     .outputMode("update") \
#     .foreachBatch(process_batch) \
#     .start()

# query.awaitTermination()

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, window, from_json, count, when, desc
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
# from pyspark.sql.functions import from_unixtime

# # 1) Create SparkSession
# spark = SparkSession.builder \
#     .appName("NowTrendingSongs") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # 2) Read from Kafka
# kafka_df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "music_events") \
#     .option("startingOffsets", "latest") \
#     .load()

# # 3) Define Schema for Incoming Data
# schema = StructType([
#     StructField("song_id", StringType(), True),
#     StructField("timestamp", DoubleType(), True),
#     StructField("region", StringType(), True),
#     StructField("action", StringType(), True)
# ])

# # Parse the JSON 'value' from Kafka
# json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
# parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data"))
# events_df = parsed_df.select("data.*")

# # 4) Convert `timestamp` to Spark TimestampType and use as event time
# events_df = events_df.withColumn("event_time", from_unixtime(col("timestamp")).cast(TimestampType()))

# # 5) Filtering "play" and "skip" actions
# plays_and_skips_df = events_df.filter(col("action").isin("like", "skip"))

# # 6) Compute Play & Skip Counts using Event Time with Watermark
# skip_ratio_df = plays_and_skips_df \
#     .withWatermark("event_time", "5 minutes") \
#     .groupBy(
#         window(col("event_time"), "10 minutes"),  # Uses event time instead of processing time
#         col("region"),
#         col("song_id")
#     ) \
#     .agg(
#         count(when(col("action") == "like", True)).alias("play_count"),
#         count(when(col("action") == "skip", True)).alias("skip_count")
#     ) \
#     .withColumn("skip_ratio", col("skip_count") / (col("play_count") + col("skip_count")))

# # 7) Filter Top Skipped Songs (Skip Ratio > 50%)
# high_skip_songs_df = skip_ratio_df.filter(col("skip_ratio") > 0.5)

# # 8) Print Results in Real-Time
# def process_batch(batch_df, batch_id):
#     if batch_df.rdd.isEmpty():
#         print("No data in this batch.")
#         return
#     print(f"=== Batch {batch_id}: Top Skipped Songs ===")
#     batch_df.orderBy(desc("skip_ratio")).show(truncate=False)

# query = high_skip_songs_df \
#     .writeStream \
#     .outputMode("update") \
#     .foreachBatch(process_batch) \
#     .start()

# query.awaitTermination()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, from_json, count, when, desc, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import from_unixtime

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

# 3) Define Schema for Incoming Data
schema = StructType([
    StructField("song_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("region", StringType(), True),
    StructField("action", StringType(), True)
])

# Parse the JSON 'value' from Kafka
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data"))
events_df = parsed_df.select("data.*")

# 4) Convert `timestamp` to Spark TimestampType and use as event time
events_df = events_df.withColumn("event_time", from_unixtime(col("timestamp")).cast(TimestampType()))

# 5) Filtering "play" and "skip" actions
plays_and_skips_df = events_df.filter(col("action").isin("play", "skip"))

# 6) Compute Play & Skip Counts using Event Time with Watermark
skip_ratio_df = plays_and_skips_df \
    .withWatermark("event_time", "5 minutes") \
    .groupBy(
        window(col("event_time"), "10 minutes"),  
        col("region"),
        col("song_id")
    ) \
    .agg(
        count(when(col("action") == "play", True)).alias("play_count"),
        count(when(col("action") == "skip", True)).alias("skip_count")
    ) \
    .withColumn("skip_ratio", col("skip_count") / (col("play_count") + col("skip_count")))

# 7) Filter Top Skipped Songs (Skip Ratio > 50%)
high_skip_songs_df = skip_ratio_df.filter(col("skip_ratio") > 0.5)

# 8) Convert to JSON format for Kafka output
output_df = high_skip_songs_df.select(
    col("window").cast("string").alias("window"),
    col("region"),
    col("song_id"),
    col("play_count"),
    col("skip_count"),
    col("skip_ratio")
).select(to_json(struct("*")).alias("value"))  # Convert entire row to JSON format

# 9) Write Stream to Kafka Topic "now_trending_results" with a Micro-Batch Interval
query = output_df \
    .writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "now_trending_results") \
    .option("checkpointLocation", "/tmp/kafka-checkpoint") \
    .trigger(processingTime='10 seconds') \
    .start()

query.awaitTermination()
