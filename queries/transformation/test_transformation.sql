--Timestamp Conversion
SELECT event_time FROM fact_user_actions LIMIT 10;

-- Validate Joins
SELECT f.action, d.category 
FROM fact_user_actions f 
JOIN dim_content d ON f.content_id = d.content_id 
LIMIT 10; 
