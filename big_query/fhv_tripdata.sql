----- Explore Biquery Features
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


----- Assignment
-- create an exteral table
CREATE OR REPLACE EXTERNAL TABLE `nyc_taxi_trips.fhv_tripdata_ext`
OPTIONS (
  format = 'CSV',
  uris = ['gs://nyc_taxi_data_lake_250123/fhv/fhv_tripdata_2019-*.csv.gz']
);

-- total rows = 43,244,696
select count(*)
from nyc_taxi_trips.fhv_tripdata_ext;

-- create big query table
CREATE OR REPLACE TABLE `nyc_taxi_trips.fhv_tripdata_non_part`
AS SELECT * FROM `nyc_taxi_trips.fhv_tripdata_ext`;


-- on external table (Cannot be estimated), resulted in 2.67 gb
SELECT COUNT(DISTINCT(affiliated_base_number)) 
FROM nyc_taxi_trips.fhv_tripdata_ext;

-- on loaded table (317 MB)
SELECT COUNT(DISTINCT(affiliated_base_number)) 
FROM nyc_taxi_trips.fhv_tripdata_non_part;

--- 638 MB on table & 2.67 GB on external table, Output is 717748 rows
SELECT COUNT(*) 
FROM nyc_taxi_trips.fhv_tripdata_non_part
where PUlocationID is NULL AND DOlocationID is NULL;

--- partition on pickup datetime & cluster on base number
SELECT count(distinct affiliated_base_number), count(distinct DATE(pickup_datetime))
FROM nyc_taxi_trips.fhv_tripdata_non_part;

--- Created optimized table
CREATE OR REPLACE TABLE `nyc_taxi_trips.fhv_tripdata_opt`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS (
  SELECT * FROM `nyc_taxi_trips.fhv_tripdata_non_part`
);

--- 647 mb will be processed
SELECT COUNT(DISTINCT(affiliated_base_number)) 
FROM nyc_taxi_trips.fhv_tripdata_non_part
where pickup_datetime between '2019-03-01' and '2019-03-31';

--- only 23 mb will be processed
SELECT COUNT(DISTINCT(affiliated_base_number)) 
FROM nyc_taxi_trips.fhv_tripdata_opt
where pickup_datetime between '2019-03-01' and '2019-03-31';
