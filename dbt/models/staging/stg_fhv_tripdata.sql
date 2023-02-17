{{ config(materialized='view') }}

with tripdata as 
(
  select *
  from {{ source('bq_ny_taxi','temp_fhv_trips') }}
)
select
    -- identifiers
    {{ dbt_utils.surrogate_key(['PUlocationID', 'pickup_datetime']) }} as tripid,
    cast(PUlocationID as integer) as  pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    SR_Flag as sr_flag,
    
    -- base number info
    Affiliated_base_number as affiliated_base_number, 
    dispatching_base_num

from tripdata

-- dbt build --m <model.sql> --var 'is_test_run: true'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}
