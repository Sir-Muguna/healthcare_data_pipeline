WITH hospital_info AS (
    SELECT
        CAST(provider_id AS STRING) AS provider_id,
        INITCAP(hospital_name) AS hospital_name,
        INITCAP(address) AS street_address,
        INITCAP(city) AS city,
        state,
        CAST(zip_code AS INTEGER) AS zip_code,
        mortality_group_measure_count,
        CAST(facility_mortaility_measures_count AS INTEGER) AS facility_mortaility_measures_count
    FROM {{ source('healthcare_raw', 'raw_hospital_general_info') }}
)

SELECT *
FROM hospital_info
WHERE provider_id IS NOT NULL -- Primary Key referencing providers table
GROUP BY provider_id, hospital_name, street_address, city, state, zip_code, mortality_group_measure_count, facility_mortaility_measures_count
