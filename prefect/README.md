


## etl using prefect

- `web_to_gcs.py` reads the csv data from web using pandas and directly writes it into google cloud storage in parquet format.

- `gcs_to_bq.py` reads the data from google cloud storage, transforms data and creates a table in biq query.


### Details steps for running prefect cloud workflow:

(only `web_to_gcs.py` & `gcs_to_bq.py` are based on GCP. Make sure GCS bucket & Biquery Dataset are created)

1. `prefect orion start`

2. `prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api` for flow runners to interact with orion

3. Create GCP & GCS credentials blocks in the Orion UI.

4. Run `prefect <flows/name-of-file.py>` to test or run a flow.

5. Create or Update `Dockerfile` with all neccessary requirements to run a flow.

6. `docker image build -t asrst/nyc-taxi-prefect-etl:v1 .` creates a local docker image.

7. `docker push asrst/nyc-taxi-prefect-etl:v1` pushes image to your repo.

8. `prefect deploy.py` deploys the flow & will be visible in orion UI.

9. `prefect deployment run etl-web-to-gcs/docker-flow` runs the flow & you can see a job in waiting stage in UI.

10. `prefect agent start -q default` creates a agent or worker which poles the queue and executes the jobs.


**Known Issue:**
 - Do not use `Service Account File or Path to the service account JSON keyfile` while creating GCP Credential block. The Deployment fails in this case as the credentials file will not be present inside the docker container. A quick fix for this is to copy-paste gcp auth json contens into the block directly & run delpoyment again. We might also solve this issue by mounting the volume in the docker block present in `deploy.py`.
