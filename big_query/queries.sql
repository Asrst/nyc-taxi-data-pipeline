-- count taxi trips in 2019 & 2020
SELECT count(*)
FROM `nyc-taxi-data-pipeline-250123.dbt_sadda.fact_trips`
where extract(year from pickup_datetime) in (2019, 2020);


-- count total fhv trips in the staging (includes trips with unknown zones)
SELECT count(*)
FROM `nyc-taxi-data-pipeline-250123.dbt_sadda.stg_fhv_tripdata`
where extract(year from pickup_datetime) = 2019;


-- count fhv trips from 2019 inner joined with zones
SELECT count(*)
FROM `nyc-taxi-data-pipeline-250123.dbt_sadda.fact_fhv_trips`
where extract(year from pickup_datetime) = 2019;


-- count fact fhv trips joined with zones in 2019, month wise
SELECT extract(month from pickup_datetime), count(*)
FROM `nyc-taxi-data-pipeline-250123.dbt_sadda.fact_fhv_trips`
where extract(year from pickup_datetime) = 2019
group by 1
order by 2;


-- count green & yellow taxi (service type) trips from 2019, 2020
SELECT service_type, count(*)
FROM `nyc-taxi-data-pipeline-250123.dbt_sadda.fact_trips`
where extract(year from pickup_datetime) in (2019, 2020)
group by service_type