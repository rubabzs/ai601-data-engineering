-- Monthly Active Users by Region
SELECT region, COUNT(DISTINCT user_id) AS active_users
FROM fact_user_actions
WHERE year='2023' AND month='09'
GROUP BY region;

-- Top Categories by Play Count
SELECT d.category, COUNT(*) AS play_count
FROM fact_user_actions f
JOIN dim_content d ON f.content_id = d.content_id
WHERE f.action = 'play'
GROUP BY d.category
ORDER BY play_count DESC
LIMIT 5;

-- Average Session Length Weekly
SELECT week, AVG(session_length_seconds) 
FROM (
  SELECT 
    session_id, 
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) AS session_length_seconds,
    WEEKOFYEAR(event_time) AS week
  FROM fact_user_actions
  GROUP BY session_id, WEEKOFYEAR(event_time)
) t
GROUP BY week;
