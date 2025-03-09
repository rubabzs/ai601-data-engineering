-- partition_raw_log_table.sql

ALTER TABLE raw_logs ADD PARTITION (year='2023', month='09', day='01') LOCATION '/raw/logs/year=2023/month=09/day=01';
ALTER TABLE raw_logs ADD PARTITION (year='2023', month='09', day='02') LOCATION '/raw/logs/year=2023/month=09/day=02';
ALTER TABLE raw_logs ADD PARTITION (year='2023', month='09', day='03') LOCATION '/raw/logs/year=2023/month=09/day=03';
ALTER TABLE raw_logs ADD PARTITION (year='2023', month='09', day='04') LOCATION '/raw/logs/year=2023/month=09/day=04';
ALTER TABLE raw_logs ADD PARTITION (year='2023', month='09', day='05') LOCATION '/raw/logs/year=2023/month=09/day=05';
ALTER TABLE raw_logs ADD PARTITION (year='2023', month='09', day='06') LOCATION '/raw/logs/year=2023/month=09/day=06';
ALTER TABLE raw_logs ADD PARTITION (year='2023', month='09', day='07') LOCATION '/raw/logs/year=2023/month=09/day=07';

