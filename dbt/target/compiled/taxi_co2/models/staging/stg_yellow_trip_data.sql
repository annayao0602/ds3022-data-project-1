

SELECT
    
    *,

    -- Alias the pickup/dropoff columns to a standard name
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,

    -- Calculate CO2 output in kgs using a subquery lookup
    (trip_distance * (SELECT co2_grams_per_mile FROM "emissions"."main"."vehicle_emissions" WHERE vehicle_type = 'Yellow') / 1000) AS trip_co2_kgs,
    
    -- Calculate average speed in MPH, protecting against division by zero
    trip_distance / (epoch(tpep_dropoff_datetime - tpep_pickup_datetime) / 3600.0) AS avg_mph,
    
    -- Extract datetime features
    EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour_of_day,
    EXTRACT(DOW FROM tpep_pickup_datetime) AS day_of_week,
    EXTRACT(WEEK FROM tpep_pickup_datetime) AS week_of_year,
    EXTRACT(MONTH FROM tpep_pickup_datetime) AS month_of_year
    
FROM "emissions"."main"."yellow_trip_data"

