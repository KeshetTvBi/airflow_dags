WITH page_view_by_hour AS(
    SELECT date_trunc(HOUR, server_ts) as hour, count(*) as page_view_count
    FROM combined_events_with_visits
    WHERE date = dateadd(day, -1, current_date())
    AND event_name = 'page_view'
    GROUP BY all
    ORDER BY hour
)
SELECT
    CASE
        WHEN COUNT(*) < 24  THEN  'FAILED'
        WHEN MIN(page_view_count) <= 0 THEN 'FAILED'
        ELSE 'SUCCESS'
        END AS tests
FROM page_view_by_hour
GROUP BY all;



