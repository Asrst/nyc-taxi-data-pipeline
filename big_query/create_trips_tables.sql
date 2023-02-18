
--- yellow taxi data
CREATE OR REPLACE EXTERNAL TABLE `nyc_taxi_trips.temp_yellow_taxi`
OPTIONS (
  format = 'CSV',
  uris = ['gs://nyc_taxi_data_lake_250123/yellow/yellow_tripdata_*.csv.gz']
);

-- CREATE OR REPLACE TABLE `nyc_taxi_trips.yellow_taxi`
-- PARTITION BY DATE(tpep_pickup_datetime) AS (
--   SELECT * FROM `nyc_taxi_trips.temp_yellow_taxi`
-- );

--- green taxi data
CREATE OR REPLACE EXTERNAL TABLE `nyc_taxi_trips.temp_green_taxi`
OPTIONS (
  format = 'CSV',
  uris = ['gs://nyc_taxi_data_lake_250123/green/green_tripdata_*.csv.gz']
);

-- CREATE OR REPLACE TABLE `nyc_taxi_trips.green_taxi`
-- PARTITION BY DATE(lpep_pickup_datetime)
-- CLUSTER BY VendorID AS (
--   SELECT * FROM `nyc_taxi_trips.green_taxi`
-- );

--- fhv trips data
CREATE OR REPLACE EXTERNAL TABLE `nyc_taxi_trips.temp_fhv_trips`
OPTIONS (
  format = 'CSV',
  uris = ['gs://nyc_taxi_data_lake_250123/fhv/fhv_tripdata_2019-*.csv.gz']
);

CREATE OR REPLACE TABLE `nyc_taxi_trips.fhv_trips`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS (
  SELECT * FROM `nyc_taxi_trips.temp_fhv_trips`
);
