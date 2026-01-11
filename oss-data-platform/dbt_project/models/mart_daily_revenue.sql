
{{ config(materialized='table') }}

WITH source_data AS (
    SELECT * FROM {{ ref('stg_nyc_taxi') }}
)

SELECT
    CAST(pickup_datetime AS DATE) AS pickup_day,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(trip_distance) AS avg_distance,
    AVG(passenger_count) AS avg_passengers
FROM source_data
GROUP BY 1
ORDER BY 1
