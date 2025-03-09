-- Populate dim_content
INSERT OVERWRITE TABLE dim_content
SELECT 
  content_id,
  title,
  category,
  length,
  artist
FROM raw_content_metadata;

-- Enable dynamic partitioning
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=1000;
SET hive.vectorized.execution.enabled=false; 

-- Populate fact_user_actions
INSERT OVERWRITE TABLE fact_user_actions PARTITION (year, month, day)
SELECT 
  user_id,
  content_id,
  action,
  `timestamp` AS event_time, 
  device,
  region,
  session_id,
  year,
  month,
  day
FROM raw_logs
WHERE 
  user_id IS NOT NULL AND 
  content_id IS NOT NULL;
