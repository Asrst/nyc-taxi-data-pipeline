from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import io


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    
    dataset_fn = f"{color}_tripdata_{year}-{month:02}"
    gcs_fp = f"{color}/{dataset_fn}.parquet"
    
    gcs_bucket = GcsBucket.load("nyc-taxi-data-lake")
    # gcs_bucket.get_directory(from_path=gcs_fp, local_path=f"../data/")

    with io.BytesIO() as buffer:
        gcs_bucket.download_object_to_file_object(gcs_fp, buffer)
        df = pd.read_parquet(buffer)

    return df


@task()
def transform(df) -> pd.DataFrame:
    """Removes invalid data"""

    if 'passenger_count' in df.columns:
        print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
        df = df[df['passenger_count'] != 0].reset_index(drop=True)
        print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    
    return df


@task()
def write_bq(df: pd.DataFrame, dest_table) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("nyc-taxi-gcp-credentials")

    df.to_gbq(
        destination_table=dest_table,
        project_id="nyc-taxi-data-pipeline-250123",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(color:str, year:int, months:list):
    """Main ETL flow to load data into Big Query"""

    for month in months:
        df = extract_from_gcs(color, year, month)
        clean_df = transform(df)

        # save to bigquery table
        dest_table = f"nyc_taxi_trips.{color}"
        write_bq(clean_df, dest_table)


if __name__ == "__main__":
    color = "yellow"
    year = 2019
    month = [2, 3]
    etl_gcs_to_bq(color, year, month)
