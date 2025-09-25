
{{ transform_trip_data(source('raw_taxi_data', 'yellow_trip_data'), 'Yellow', 'tpep') }}