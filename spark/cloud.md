## Running Spark in the Cloud

### Connecting to Google Cloud Storage 

Uploading data to GCS:

```bash
gsutil -m cp -r pq/ gs://nyc_taxi_data_lake_250123/pq
```

Download the jar for connecting to GCS to any location (e.g. the `lib` folder):

```bash
gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar
```

See the notebook with configuration in [pyspark_gcs.ipynb](pyspark_gcs.ipynb)

(Thanks Alvin Do for the instructions!)


### Local Cluster and Spark-Submit

Creating a stand-alone cluster ([docs](https://spark.apache.org/docs/latest/spark-standalone.html)):

```bash
./sbin/start-master.sh
```

Creating a worker:

```bash
URL="spark://de-zoomcamp.asia-south1-a.de-zoomcamp-nytaxi.internal:7077"
./sbin/start-slave.sh ${URL}

# for newer versions of spark use that:
#./sbin/start-worker.sh ${URL}
```

If required turn the notebook into a script:

```bash
jupyter nbconvert --to=script <path/to/notebook>.ipynb
```

Edit the script and then run it:

```bash 
python taxi_trips_to_pq.py \
    --input_green=data/pq/green/2020/*/ \
    --input_yellow=data/pq/yellow/2020/*/ \
    --output=data/report-2020
```

Use `spark-submit` for running the script on the cluster

```bash
URL="spark://de-zoomcamp.asia-south1-a.de-zoomcamp-nytaxi.internal:7077"

spark-submit \
    --master="${URL}" \
    taxi_trips_to_pq.py \
        --input_green=data/pq/green/2021/*/ \
        --input_yellow=data/pq/yellow/2021/*/ \
        --output=data/report-2021
```

### Data Proc

Upload the script to GCS:

```bash
TODO
```

Params for the job:

* `--input_green=gs://nyc_taxi_data_lake_250123 /pq/green/2021/*/`
* `--input_yellow=gs://nyc_taxi_data_lake_250123 /pq/yellow/2021/*/`
* `--output=gs://nyc_taxi_data_lake_250123 /report-2021`


Using Google Cloud SDK for submitting to dataproc
([link](https://cloud.google.com/dataproc/docs/guides/submit-job#dataproc-submit-job-gcloud))

```bash
gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=asia-south1 \
    gs://nyc_taxi_data_lake_250123 /code/taxi_trips_to_pq.py \
    -- \
        --input_green=gs://nyc_taxi_data_lake_250123 /pq/green/2020/*/ \
        --input_yellow=gs://nyc_taxi_data_lake_250123 /pq/yellow/2020/*/ \
        --output=gs://nyc_taxi_data_lake_250123 /report-2020
```


### Big Query

Upload the script to GCS:

```bash
TODO
```

Write results to big query ([docs](https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example#pyspark)):

```bash
gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=asia-south1 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar
    gs://nyc_taxi_data_lake_250123 /code/taxi_trips_to_bq.py \
    -- \
        --input_green=gs://nyc_taxi_data_lake_250123 /pq/green/2020/*/ \
        --input_yellow=gs://nyc_taxi_data_lake_250123 /pq/yellow/2020/*/ \
        --output=trips_data_all.report_2020
```

