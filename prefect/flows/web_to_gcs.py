from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta
from random import randint
import io


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    
    return df


@task(log_prints=True)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""

    if 'tpep_pickup_datetime' in df.columns:
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    elif 'lpep_dropoff_datetime' in df.columns:
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")

    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(df, save_path) -> None:
    """Upload local parquet file to GCS"""
    
    # gcs_block = GcsBucket.load("zoom-gcs")
    # gcs_block.upload_from_path(from_path=path, to_path=path)


    gcs_block = GcsBucket.load("nyc-taxi-data-lake")
    buffer = io.BytesIO(df.to_parquet(compression="gzip"))
    gcs_block.upload_from_file_object(buffer, to_path=save_path)

    return


@flow()
def etl_web_to_gcs(color:str='green', year:int=2020, months:list=[5]) -> None:
    """The main ETL function"""

    for month in months:
        dataset_fn = f"{color}_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_fn}.csv.gz"

        df = fetch(dataset_url)
        df_clean = transform(df)
        
        # skipping this step
        # path = write_local(df_clean, color, dataset_file)

        bucket_path = f"{color}/{dataset_fn}.parquet"
        write_gcs(df_clean, bucket_path)


if __name__ == "__main__":
    color = "green"
    year = 2019
    month = [4]
    etl_web_to_gcs(color, year, month)

