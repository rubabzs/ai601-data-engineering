#!/bin/bash

DATE=$1
YEAR=$(date -d $DATE +%Y)
MONTH=$(date -d $DATE +%m)
DAY=$(date -d $DATE +%d)

# Directories in HDFS
LOG_DIR=/raw/logs/$YEAR/$MONTH/$DAY
META_DIR=/raw/metadata/$YEAR/$MONTH/$DAY

# Create directories
hdfs dfs -mkdir -p $LOG_DIR
hdfs dfs -mkdir -p $META_DIR

# Go to raw_data folder where CSV files are stored
cd /root/raw_data

# Put data into HDFS
hdfs dfs -put $DATE.csv $LOG_DIR/
hdfs dfs -put content_metadata.csv $META_DIR/
