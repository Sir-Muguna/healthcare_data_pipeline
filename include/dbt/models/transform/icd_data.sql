WITH icd_raw_data AS (
    SELECT
        *
    FROM
        {{ source('healthcare_raw', 'raw_icd_data') }}  
)

SELECT
    chapter,
    INITCAP(chapter_description) AS chapter_description,
    code_category,
    INITCAP(code_category_description) AS code_category_description,
    icd_code AS icd_category,
    
    -- Clean up and format disease_description: trim spaces, replace () with [], and apply title case
    INITCAP(REPLACE(REPLACE(TRIM(disease_description), '(', '['), ')', ']')) AS icd_category_description
FROM
    icd_raw_data
WHERE
    icd_code IS NOT NULL -- Primary Key referencing providers table
GROUP BY
    chapter,
    chapter_description,
    code_category,
    code_category_description,
    icd_code,
    icd_category_description
