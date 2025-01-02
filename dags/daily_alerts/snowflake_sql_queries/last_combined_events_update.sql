SELECT
    CASE
        WHEN COUNT(DISTINCT date) = 1 THEN 'SUCCESS'
        ELSE 'FAILED'
    END AS last_combined_events_update
FROM combined_events_with_visits
WHERE date >= CURRENT_DATE -1 ;