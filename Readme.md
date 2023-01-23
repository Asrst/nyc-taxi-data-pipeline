
# nyc-taxi-data-pipeline

`python3 ingest_data.py --user root --password root --host 0.0.0.0 --port 5432 --db ny_taxi --table_name yellow_taxi --url https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2020-01.csv.gz`


`python3 ingest_data.py --user root --password root --host 0.0.0.0 --port 5432 --db ny_taxi --table_name taxi_zones --url https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv`


`python3 ingest_trips.py --user root --password root --host 0.0.0.0 --port 5432 --db ny_taxi --table_name green_taxi --url https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz`



