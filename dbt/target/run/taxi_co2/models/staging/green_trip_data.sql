
  
  create view "emissions"."main"."green_trip_data__dbt_tmp" as (
    



    


SELECT
    
    *,

    -- standard names for pickup and dropoff datetime
    lpep_pickup_datetime AS pickup_datetime,
    lpep_dropoff_datetime AS dropoff_datetime,

    -- Calculate CO2 output in kgs
    (trip_distance * (SELECT co2_grams_per_mile FROM "emissions"."main"."vehicle_emissions" WHERE vehicle_type = 'green_taxi') / 1000) AS trip_co2_kgs,
        
    -- Calculate average speed in MPH
    trip_distance / (epoch(lpep_dropoff_datetime - lpep_pickup_datetime) / 3600.0) AS avg_mph,
    
    -- Extract datetime features
    EXTRACT(HOUR FROM lpep_pickup_datetime) AS hour_of_day,
    EXTRACT(DOW FROM lpep_pickup_datetime) AS day_of_week,
    EXTRACT(WEEK FROM lpep_pickup_datetime) AS week_of_year,
    EXTRACT(MONTH FROM lpep_pickup_datetime) AS month_of_year

    
FROM "emissions"."main"."green_trip_data"


  );
