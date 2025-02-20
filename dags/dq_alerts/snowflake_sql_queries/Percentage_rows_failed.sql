WITH fail AS (
    SELECT
        file_name,
        DATE(last_load_time) AS load_date,
        row_count,
        row_parsed,
        error_count,
        status,
        file_size,
        table_name
    FROM snowflake.account_usage.copy_history
    WHERE DATE(last_load_time) = CURRENT_DATE()
    AND FILE_NAME like '%/validated/ready/%'
    AND TABLE_SCHEMA_NAME = 'PUBLIC'
    AND TABLE_CATALOG_NAME = 'MAKO_DATA_LAKE'
    AND status != 'Load failed'
)

SELECT
    CASE
        WHEN   COALESCE(COALESCE(sum (error_count), 0)*100 /  sum (row_parsed),0)  < 1 THEN 'SUCCESS'
        ELSE 'The percentage of rows that were not loaded is: ' || COALESCE(sum (error_count), 0) * 100/  sum(row_parsed)
    END AS result
FROM fail;