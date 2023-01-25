
## nyc-taxi-data-pipeline

1. Run the following command to start postgres server & pg admin (requires docker & docker-compose).
    
    `docker-compose up`


2. Open the terminal and run below scripts to download and ingest data to local postgress instance.

    ```python
    
    python3 ingest_data.py --user root --password root --host 0.0.0.0 --port 5432 --db ny_taxi --table_name yellow_taxi --url https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2020-01.csv.gz

    python3 ingest_trips.py --user root --password root --host 0.0.0.0 --port 5432 --db ny_taxi --table_name green_taxi --url https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz

    python3 ingest_data.py --user root --password root --host 0.0.0.0 --port 5432 --db ny_taxi --table_name taxi_zones --url https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
    ```


3. Run the queries in pg-admin to query the data.

    ```sql

    select count(*)
    from green_taxi
    where date(lpep_pickup_datetime) = '2019-01-15'
    and date(lpep_dropoff_datetime) = '2019-01-15';


    select date(lpep_pickup_datetime), max(trip_distance)
    from green_taxi
    group by date(lpep_pickup_datetime)
    order by 2 desc;


    select passenger_count, count(*)
    from green_taxi
    where date(lpep_pickup_datetime) = '2019-01-01'
    or date(lpep_dropoff_datetime) = '2019-01-01'
    group by passenger_count;


    select dz."Zone", max(tip_amount)
    from green_taxi gt
    inner join taxi_zones pz
    on gt."PULocationID" = pz."LocationID"
    left join taxi_zones dz
    on gt."DOLocationID" = dz."LocationID"
    where pz."Zone" ilike '%astoria%'
    group by dz."Zone"
    order by 2 desc;
    ```
