#!/bin/bash

# Check if date parameter is provided
if [ -z "$1" ]; then
    echo "❌ Error: Date parameter is missing. Usage: $0 YYYY-MM-DD"
    exit 1
fi

DATE=$1
YEAR=$(echo $DATE | cut -d'-' -f1)
MONTH=$(echo $DATE | cut -d'-' -f2)
DAY=$(echo $DATE | cut -d'-' -f3)

echo "Year: $YEAR"
echo "Month: $MONTH"
echo "Day: $DAY"

# Define local logs directory
LOGS_DIR="logs_backup/$DATE"
NO_DATE_DIR="logs_backup"

# Check if local directory exists
if [ ! -d "$LOGS_DIR" ]; then
    echo "❌ Error: Local directory does not exist: $LOGS_DIR"
    exit 1
fi

# Check if user.csv exists in the local directory
if [ ! -f "$LOGS_DIR/user_logs.csv" ]; then
    echo "❌ Error: user.csv file not found in: $LOGS_DIR"
    exit 1
fi

# Define HDFS paths
HDFS_LOGS_DIR="/raw/logs/$YEAR/$MONTH/$DAY"
HDFS_METADATA_DIR="/raw/metadata/$YEAR/$MONTH/$DAY"

# Create directories in HDFS
echo "Creating HDFS directories..."
hdfs dfs -mkdir -p $HDFS_LOGS_DIR
hdfs dfs -mkdir -p $HDFS_METADATA_DIR

# Copy log files to HDFS
echo "Copying user.csv to HDFS..."
hdfs dfs -put "$LOGS_DIR/user_logs.csv" "$HDFS_LOGS_DIR/"

echo "Copying content_metadata.csv to HDFS..."
hdfs dfs -put "$NO_DATE_DIR/content_metadata.csv" "$HDFS_METADATA_DIR/"


echo "✅ Logs copied to HDFS: $HDFS_LOGS_DIR"
echo "✅ Logs copied to HDFS: $HDFS_METADATA_DIR"
