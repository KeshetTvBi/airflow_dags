SELECT
    CASE
        WHEN MAX(last_mod_time) >= CURRENT_DATE - 1 THEN 'SUCCESS'
        ELSE 'FAILED'
    END AS last_dataenrichment_update
FROM dataenrichment ;