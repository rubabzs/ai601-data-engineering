#!/bin/bash

DATE=$1

# Extract year, month, and day
YEAR=$(echo $DATE | cut -d'-' -f1)
MONTH=$(echo $DATE | cut -d'-' -f2)
DAY=$(printf "%02d" $(echo $DATE | cut -d'-' -f3))

# Define HDFS paths
LOGS_HDFS_PATH="/raw/logs/$YEAR/$MONTH/$DAY/"
METADATA_HDFS_PATH="/raw/metadata/"

# Define local paths (where logs & metadata are stored before uploading)
LOCAL_LOGS_PATH="user_logs_$DATE.csv"
LOCAL_METADATA_PATH="content_metadata.csv"

# Check if date parameter is provided
if [ -z "$DATE" ]; then
    echo "Error: Please provide a date in YYYY-MM-DD format."
    exit 1
fi

echo "Ingesting logs for date: $DATE"

# Create directories in HDFS if they don’t exist
hdfs dfs -mkdir -p $LOGS_HDFS_PATH
hdfs dfs -mkdir -p $METADATA_HDFS_PATH

# Move user logs to HDFS (Partitioned by year/month/day)
if [ -f "$LOCAL_LOGS_PATH" ]; then
    echo "Uploading $LOCAL_LOGS_PATH to HDFS: $LOGS_HDFS_PATH"
    hdfs dfs -put -f "$LOCAL_LOGS_PATH" "$LOGS_HDFS_PATH"
else
    echo "Warning: $LOCAL_LOGS_PATH not found!"
fi

# Move content metadata to HDFS (Only 1 copy at /raw/metadata/)
if [ -f "$LOCAL_METADATA_PATH" ]; then
    echo "Uploading metadata to HDFS: $METADATA_HDFS_PATH"
    hdfs dfs -put -f "$LOCAL_METADATA_PATH" "$METADATA_HDFS_PATH"
else
    echo "Warning: $LOCAL_METADATA_PATH not found!"
fi

echo "Ingestion complete!"

