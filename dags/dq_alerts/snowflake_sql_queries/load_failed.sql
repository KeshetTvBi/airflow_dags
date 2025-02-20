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
    AND status = 'Load failed'
)

SELECT
    CASE
        WHEN COUNT(*) = 0 THEN 'SUCCESS'
        ELSE COUNT(*) || ' files not loaded. Files: ' || LISTAGG(file_name, ', ')
    END AS result
FROM fail;