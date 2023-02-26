{{ config(materialized='view') }}
select
    -- identifiers
    cast(PUlocationID as integer) as  pickup_location_id,
    cast(DOlocationID as integer) as dropoff_location_id,
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    -- trip info
    dispatching_base_num as dispatching_base_id,
    affiliated_base_number as affiliated_base_id,
    sr_flag
from {{ source('staging','fhv_trip_data') }}
{% if var('is_test_run', default=true) %}
  limit 100
{% endif %}
