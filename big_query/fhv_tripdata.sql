-- creates an external table
CREATE OR REPLACE EXTERNAL TABLE `nyc_taxi_trips.fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://nyc-tl-data/trip data/fhv_tripdata_2019-*.csv']
);


SELECT count(*) FROM `nyc_taxi_trips.fhv_tripdata`;


SELECT COUNT(DISTINCT(dispatching_base_num)) FROM `nyc_taxi_trips.fhv_tripdata`;

--- creates a table in BQ & loads the data (better & faster than external table)
CREATE OR REPLACE TABLE `nyc_taxi_trips.fhv_nonpartitioned_tripdata`
AS SELECT * FROM `nyc_taxi_trips.fhv_tripdata`;

--- created a optimized table (better than non partition tables when data is large)
CREATE OR REPLACE TABLE `nyc_taxi_trips.fhv_partitioned_tripdata`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS (
  SELECT * FROM `nyc_taxi_trips.fhv_tripdata`
);

SELECT count(*) FROM  `nyc_taxi_trips.fhv_nonpartitioned_tripdata`
WHERE dropoff_datetime BETWEEN '2019-01-01' AND '2019-03-31'
  AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');


SELECT count(*) FROM `nyc_taxi_trips.fhv_partitioned_tripdata`
WHERE dropoff_datetime BETWEEN '2019-01-01' AND '2019-03-31'
  AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');