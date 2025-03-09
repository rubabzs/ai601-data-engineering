-- Dimension table for content metadata
DROP TABLE dim_content;

CREATE TABLE dim_content (
    content_id INT,
    title STRING,
    category STRING,
    length INT,
    artist STRING
)
STORED AS PARQUET;

DROP TABLE fact_user_actions;

CREATE TABLE fact_user_actions (
    user_id INT,
    content_id INT,
    action STRING,
    event_time TIMESTAMP,  -- Corrected column name
    device STRING,
    region STRING,
    session_id STRING
)
PARTITIONED BY (year STRING, month STRING, day STRING)
STORED AS PARQUET;
