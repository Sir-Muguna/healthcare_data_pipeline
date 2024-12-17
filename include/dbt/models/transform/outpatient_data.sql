WITH combined_outpatient_charges AS (
    SELECT 
        CAST(provider_id AS STRING) AS provider_id,  -- Foreign Key referencing providers table
        INITCAP(provider_name) AS provider_name,
        CAST(2011 AS STRING) AS year,
        INITCAP(provider_street_address) AS provider_street_address,
        INITCAP(provider_city) AS provider_city,
        UPPER(provider_state) AS provider_state,
        CAST(provider_zipcode AS INTEGER) AS provider_zipcode,
        apc,
        outpatient_services,
        average_estimated_submitted_charges,
        average_total_payments
    FROM {{ source('healthcare_raw', 'raw_outpatient_charges_2011') }}

    UNION ALL

    SELECT 
        CAST(provider_id AS STRING) AS provider_id,  -- Foreign Key referencing providers table
        INITCAP(provider_name) AS provider_name,
        CAST(2012 AS STRING) AS year,
        INITCAP(provider_street_address) AS provider_street_address,
        INITCAP(provider_city) AS provider_city,
        UPPER(provider_state) AS provider_state,
        CAST(provider_zipcode AS INTEGER) AS provider_zipcode,
        apc,
        outpatient_services,
        average_estimated_submitted_charges,
        average_total_payments
    FROM {{ source('healthcare_raw', 'raw_outpatient_charges_2012') }}

    UNION ALL

    SELECT 
        CAST(provider_id AS STRING) AS provider_id,  -- Foreign Key referencing providers table
        INITCAP(provider_name) AS provider_name,
        CAST(2013 AS STRING) AS year,
        INITCAP(provider_street_address) AS provider_street_address,
        INITCAP(provider_city) AS provider_city,
        UPPER(provider_state) AS provider_state,
        CAST(provider_zipcode AS INTEGER) AS provider_zipcode,
        apc,
        outpatient_services,
        average_estimated_submitted_charges,
        average_total_payments
    FROM {{ source('healthcare_raw', 'raw_outpatient_charges_2013') }}
)

SELECT *
FROM combined_outpatient_charges

