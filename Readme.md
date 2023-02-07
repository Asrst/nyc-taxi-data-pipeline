
## nyc-taxi-data-pipeline

**prerequisites**: 
- Install Requirements using requirements.txt
- GCP Account & Terraform (for cloud workflows)
- Make sure Docker is installed & Logged in to the Docker CLI using `docker login`


### steps for cloud workflow

1. Run terraform to create/update GCp infra (refer `/terraform`).
2. Run the flows in the `prefect/flows` to test & Deploy them by creating a docker image (refer `/prefect`). 


**Note:** Refer branches `week1`, `week2-prefect-intro` or `week2-prefect-etl` for local flow based on postgres.