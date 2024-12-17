

SELECT
    *
FROM
    EXTERNAL_QUERY('healthcare-data-pipeline', 'SELECT * FROM `healthcare-data-pipeline.healthcare.icd_raw_data`')