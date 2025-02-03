WITH device_data AS (
    SELECT device_data['device_type']::STRING as device_type, count(distinct user_id) as user_count
    FROM combined_events_with_visits
    WHERE date = DATEADD(DAY, -1, CURRENT_DATE())
    AND device_type != 'Unknown'
    GROUP BY all
)
SELECT
    CASE
        WHEN COUNT(*) < 4  THEN  'FAILED'
        WHEN MIN(user_count) <= 0 THEN 'FAILED'
        ELSE 'SUCCESS'
    END AS tests
FROM device_data
GROUP BY all;
