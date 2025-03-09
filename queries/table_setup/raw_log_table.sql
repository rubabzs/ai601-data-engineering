DROP TABLE raw_logs;

CREATE EXTERNAL TABLE raw_logs (
  user_id INT,
  content_id INT,
  action STRING,
  `timestamp` STRING,
  device STRING,
  region STRING,
  session_id STRING
)
PARTITIONED BY (year STRING, month STRING, day STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/raw/logs'
TBLPROPERTIES ("skip.header.line.count"="1"); 
