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
        WHEN MIN(page_view_count) > 100000 THEN 'SUCCESS'
        ELSE 'FAILED' END AS page_view_count
FROM page_view_by_hour
GROUP BY all;