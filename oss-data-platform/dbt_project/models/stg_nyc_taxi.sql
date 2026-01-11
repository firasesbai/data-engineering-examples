
{{ config(materialized='view') }}

SELECT 
    CAST(pickup_datetime AS TIMESTAMP) as pickup_datetime,
    CAST(dropoff_datetime AS TIMESTAMP) as dropoff_datetime,
    CAST(pickup_longitude AS DOUBLE) as pickup_longitude,
    CAST(pickup_latitude AS DOUBLE) as pickup_latitude,
    CAST(dropoff_longitude AS DOUBLE) as dropoff_longitude,
    CAST(dropoff_latitude AS DOUBLE) as dropoff_latitude,
    CAST(passenger_count AS INTEGER) as passenger_count,
    CAST(trip_distance AS DOUBLE) as trip_distance,
    CAST(fare_amount AS DOUBLE) as fare_amount,
    CAST(tip_amount AS DOUBLE) as tip_amount,
    CAST(total_amount AS DOUBLE) as total_amount
    -- Add other columns if needed, these are the ones used in the mart
FROM read_csv_auto('s3://data-lake/landing/nyc_taxi_sample.csv', header=True)
