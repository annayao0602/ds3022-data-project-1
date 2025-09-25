-- macros/transformations.sql

{% macro transform_trip_data(source_table, vehicle_type, prefix) %}

SELECT
    
    *,

    -- standard names for pickup and dropoff datetime
    {{ prefix }}_pickup_datetime AS pickup_datetime,
    {{ prefix }}_dropoff_datetime AS dropoff_datetime,

    -- Calculate CO2 output in kgs
    (trip_distance * (SELECT co2_grams_per_mile FROM {{ source('raw_taxi_data', 'vehicle_emissions') }} WHERE vehicle_type = '{{ vehicle_type }}') / 1000) AS trip_co2_kgs,
    
    -- Calculate average speed in MPH
    trip_distance / (epoch({{ prefix }}_dropoff_datetime - {{ prefix }}_pickup_datetime) / 3600.0) AS avg_mph,
    
    -- Extract datetime features
    EXTRACT(HOUR FROM {{ prefix }}_pickup_datetime) AS hour_of_day,
    EXTRACT(DOW FROM {{ prefix }}_pickup_datetime) AS day_of_week,
    EXTRACT(WEEK FROM {{ prefix }}_pickup_datetime) AS week_of_year,
    EXTRACT(MONTH FROM {{ prefix }}_pickup_datetime) AS month_of_year
    
FROM {{ source_table }}

{% endmacro %}