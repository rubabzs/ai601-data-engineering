#!/bin/bash
DATE=$1

# Validate input date format
if [[ ! $DATE =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "Error: Invalid date format. Use YYYY-MM-DD"
    exit 1
fi

# Parse date components
YEAR=$(date -d "$DATE" +%Y 2>/dev/null) || { echo "Invalid date"; exit 1; }
MONTH=$(date -d "$DATE" +%m)
DAY=$(date -d "$DATE" +%d)

# HDFS Paths using Hive-compatible naming
HDFS_LOG_DIR="/raw/logs/year=$YEAR/month=$MONTH/day=$DAY"
HDFS_META_DIR="/raw/metadata"

# Local paths
LOCAL_LOG_DIR="$HOME/raw_data/logs/$YEAR/$MONTH/$DAY"
LOCAL_META_FILE="$HOME/raw_data/metadata/2023/09/content_metadata.csv"

# Remove only the specific day's directory (silent if it doesn't exist)
hdfs dfs -rm -r $HDFS_LOG_DIR >/dev/null 2>&1

# Create HDFS directory for logs
hdfs dfs -mkdir -p $HDFS_LOG_DIR

# Check and copy logs
if [ -d "$LOCAL_LOG_DIR" ] && [ -n "$(ls -A $LOCAL_LOG_DIR/*.csv 2>/dev/null)" ]; then
    hdfs dfs -put $LOCAL_LOG_DIR/*.csv $HDFS_LOG_DIR/
else
    echo "Warning: No CSV files found in $LOCAL_LOG_DIR"
fi

# Copy metadata once (static copy)
if [ ! -f $LOCAL_META_FILE ]; then
    echo "Error: Metadata file not found at $LOCAL_META_FILE"
    exit 1
fi

hdfs dfs -test -e $HDFS_META_DIR/content_metadata.csv
if [ $? -ne 0 ]; then
    hdfs dfs -mkdir -p $HDFS_META_DIR
    hdfs dfs -put $LOCAL_META_FILE $HDFS_META_DIR/
fi

echo "Successfully ingested logs for $DATE"
