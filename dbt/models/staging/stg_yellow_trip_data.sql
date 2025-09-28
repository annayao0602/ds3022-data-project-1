
{{
  config(
    post_hook=[
      "{{ log('Successfully finished building stg_yellow_trip_data.', info=True) }}"
    ]
  )
}}

{{ transform_trip_data(source('raw_taxi_data', 'yellow_trip_data'), 'yellow_taxi', 'tpep') }}

