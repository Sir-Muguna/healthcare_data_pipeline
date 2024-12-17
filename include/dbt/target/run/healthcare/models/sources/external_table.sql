
  
    

    create or replace table `healthcare-data-pipeline`.`healthcare`.`external_table`
      
    
    

    OPTIONS()
    as (
      

SELECT
    *
FROM
    EXTERNAL_QUERY('healthcare-data-pipeline', 'SELECT * FROM `healthcare-data-pipeline.healthcare.icd_raw_data`')
    );
  