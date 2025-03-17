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

# Convert timestamp double -> actual timestamp for event time processing
events_df = events_df.withColumn("event_time", 
                               (col("timestamp")).cast(TimestampType()))
# Add watermark for event time window
events_df = events_df.withWatermark("event_time", "5 minutes")

# 4) Process all actions (plays, skips, likes)
# 5) Group by region + 2-minute event time window (changed from 5 to 2 minutes)
windowed_df = events_df \
    .groupBy(
        window(col("event_time"), "2 minutes"),  # Changed from 5 to 2 minutes
        col("region"),
        col("song_id")
    ) \
    .agg(
        count(when(col("action") == "play", 1)).alias("play_count"),
        count(when(col("action") == "skip", 1)).alias("skip_count"),
        count(when(col("action") == "like", 1)).alias("like_count")
    ) \
    .withColumn("skip_ratio", 
                col("skip_count") / (col("play_count") + col("skip_count") + 0.1))

# 6) Use foreachBatch to do rank-based top N logic each micro-batch
def process_batch(batch_df, batch_id):
    """
    This function is called for each micro-batch. We treat 'batch_df' as a normal batch DataFrame.
    We'll rank by 'play_count' within each region & window and pick top 3.
    """
    if batch_df.rdd.isEmpty():
        print("No data in this batch.")
        return

    # We'll partition by region + the 'window' column
    w = Window.partitionBy("region", "window").orderBy(desc("play_count"))

    ranked_df = batch_df.withColumn("rn", row_number().over(w)) \
                        .filter(col("rn") <= 3)

    # Show the top songs for each region + window
    print(f"=== Batch: {batch_id} ===")
    ranked_df.show(truncate=False)
    
    # Write results to a new Kafka topic
    ranked_df.select(
        "window", "region", "song_id", "play_count", "skip_count", "like_count", "skip_ratio"
    ).selectExpr("to_json(struct(*)) AS value") \
      .write \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092") \
      .option("topic", "now_trending_results") \
      .save()

# 7) Write Stream with foreachBatch and a different micro-batch interval
query = windowed_df.writeStream \
    .outputMode("update") \
    .trigger(processingTime='3 seconds') \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()