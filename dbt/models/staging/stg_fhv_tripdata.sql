{{config(
    materialized = "table")
}}

select *
from {{source('bq_ny_taxi', 'fhv_tripdata_opt') }}
where pickup_datetime between '2019-03-01' and '2019-03-31'