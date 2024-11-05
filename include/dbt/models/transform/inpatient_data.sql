WITH combined_inpatient_data AS (
    SELECT
        CAST(provider_id AS STRING) AS provider_id, -- Foreign Key referencing providers table
        icd_category AS icd_category,
        CAST(2011 AS STRING) AS year,
        INITCAP(provider_name) AS provider_name,
        INITCAP(provider_street_address) AS provider_street_address,
        INITCAP(provider_city) AS provider_city,
        UPPER(provider_state) AS provider_state, 
        CAST(provider_zipcode AS INTEGER) AS provider_zipcode,
        CAST(total_discharges AS INTEGER) AS total_discharges,
        average_covered_charges AS average_covered_charges,
        average_total_payments AS average_total_payments,
        average_medicare_payments AS average_medicare_payments
    FROM {{ source('healthcare_raw', 'raw_inpatient_2011') }}

    UNION ALL

    SELECT
        CAST(provider_id AS STRING) AS provider_id, -- Foreign Key referencing providers table
        icd_category AS icd_category,
        CAST(2012 AS STRING) AS year,
        INITCAP(provider_name) AS provider_name,
        INITCAP(provider_street_address) AS provider_street_address,
        INITCAP(provider_city) AS provider_city,
        UPPER(provider_state) AS provider_state,
        CAST(provider_zipcode AS INTEGER) AS provider_zipcode,
        CAST(total_discharges AS INTEGER) AS total_discharges,
        average_covered_charges AS average_covered_charges,
        average_total_payments AS average_total_payments,
        average_medicare_payments AS average_medicare_payments
    FROM {{ source('healthcare_raw', 'raw_inpatient_2012') }}

    UNION ALL

    SELECT
        CAST(provider_id AS STRING) AS provider_id, -- Foreign Key referencing providers table
        icd_category AS icd_category,
        CAST(2013 AS STRING) AS year,
        INITCAP(provider_name) AS provider_name,
        INITCAP(provider_street_address) AS provider_street_address,
        INITCAP(provider_city) AS provider_city,
        UPPER(provider_state) AS provider_state,
        CAST(provider_zipcode AS INTEGER) AS provider_zipcode,
        CAST(total_discharges AS INTEGER) AS total_discharges,
        average_covered_charges AS average_covered_charges,
        average_total_payments AS average_total_payments,
        average_medicare_payments AS average_medicare_payments
    FROM {{ source('healthcare_raw', 'raw_inpatient_2013') }}
)

SELECT *
FROM combined_inpatient_data
