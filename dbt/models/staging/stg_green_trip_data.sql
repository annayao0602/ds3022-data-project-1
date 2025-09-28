{{
  config(
    post_hook=[
      "{{ log('Successfully finished building stg_green_trip_data.', info=True) }}"
    ]
  )
}}

{{ transform_trip_data(source('raw_taxi_data', 'green_trip_data'), 'green_taxi', 'lpep') }}

