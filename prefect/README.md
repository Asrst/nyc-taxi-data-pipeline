


## etl using prefect

- `load_fhv.py` reads the csv NY Taxi's for Hire Vehicle's (FHV) data from web using pandas and directly writes it into google cloud storage in compressed gz format (total of 43,244,696 records for 2019).


### Details steps for running prefect cloud workflow:

(This flow is based on GCP. Make sure GCS bucket is created)

1. `prefect orion start`

2. `prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api` for flow runners to interact with orion

3. Create GCP & GCS credentials blocks in the Orion UI.

4. Run `prefect <flows/name-of-file.py>` to test or run a flow.

5. Create or Update `Dockerfile` with all neccessary requirements to run a flow.

6. `docker image build -t asrst/nyc-taxi-prefect-etl:v1 .` creates a local docker image.

7. `docker push asrst/nyc-taxi-prefect-etl:v1` pushes image to your repo.

8. `prefect deploy.py` deploys the flow & will be visible in orion UI.

9. `prefect deployment run load-fhv-to-gcs/docker-flow` runs the flow & you can see a job in waiting stage in UI.

10. `prefect agent start -q default` creates a agent or worker which poles the queue and executes the jobs.


**Known Issue:**
 - Do not use `Service Account File or Path to the service account JSON keyfile` while creating GCP Credential block. The Deployment fails in this case as the credentials file will not be present inside the docker container. A quick fix for this is to copy-paste gcp auth json contens into the block directly & run delpoyment again. We might also solve this issue by mounting the volume in the docker block present in `deploy.py`.
