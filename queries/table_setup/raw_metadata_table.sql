DROP TABLE raw_content_metadata;

CREATE EXTERNAL TABLE raw_content_metadata (
  content_id INT,
  title STRING,
  category STRING,
  length INT,
  artist STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/raw/metadata'
TBLPROPERTIES ("skip.header.line.count"="1");
