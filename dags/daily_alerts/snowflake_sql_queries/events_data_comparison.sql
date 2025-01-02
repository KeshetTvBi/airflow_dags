WITH yesterday AS(
    SELECT event_name, count(*) AS yesterday
    FROM COMBINED_EVENTS_WITH_VISITS
    WHERE date = DATEADD(DAY, -1, CURRENT_DATE())
    GROUP BY event_name
),
the_day_before_yesterday AS(
    SELECT event_name, count(*) AS the_day_before_yesterday
    FROM COMBINED_EVENTS_WITH_VISITS
    WHERE date = DATEADD(DAY, -2, CURRENT_DATE())
    GROUP BY event_name
)
SELECT
    CASE
        WHEN MAX(ABS(((yesterday / the_day_before_yesterday) - 1) * 100)) <= 10 THEN  'SUCCESS'
        ELSE 'FAILED'
    END AS presanteges
FROM yesterday AS a FULL OUTER JOIN the_day_before_yesterday AS b ON a.event_name = b.event_name
WHERE a.event_name IN('ads', 'play', 'click', 'page_view')