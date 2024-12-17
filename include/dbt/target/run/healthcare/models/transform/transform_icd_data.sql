
  
    

    create or replace table `healthcare-data-pipeline`.`healthcare`.`transform_icd_data`
      
    
    

    OPTIONS()
    as (
      WITH icd_raw_data AS (
    SELECT
        *
    FROM
        `healthcare-data-pipeline`.`healthcare`.`raw_icd_data`  -- Update to use the source function
)

SELECT
    -- Select and transform each column as specified
    code_category,
    code_category_description,
    
    -- Include only rows where icd_code is not null
    icd_code,
    
    -- Clean up disease_description: trim spaces and replace () with []
    REPLACE(
        REPLACE(TRIM(disease_description), '(', '['), ')', ']'
    ) AS disease_description
FROM
    icd_raw_data
WHERE
    icd_code IS NOT NULL
    );
  