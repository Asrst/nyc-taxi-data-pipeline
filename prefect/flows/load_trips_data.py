from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta
from random import randint
import io, gc


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    
    return df


def get_type_map(columns):

    type_map = {
                    'VendorID': 'Int64',
                    'tpep_pickup_datetime': 'datetime64',
                    'tpep_dropoff_datetime': 'datetime64',
                    'lpep_pickup_datetime': 'datetime64',
                    'lpep_dropoff_datetime': 'datetime64',
                    'passenger_count': 'Int64',
                    'trip_distance': 'float64',
                    'RatecodeID': 'Int64',
                    'store_and_fwd_flag': 'object',
                    'PULocationID': 'Int64',
                    'DOLocationID': 'Int64',
                    'payment_type': 'Int64',
                    'fare_amount': 'float64',
                    'extra': 'float64',
                    'mta_tax': 'float64',
                    'tip_amount': 'float64',
                    'tolls_amount': 'float64',
                    'improvement_surcharge': 'float64',
                    'total_amount': 'float64',
                    'congestion_surcharge': 'float64',
                    'pickup_datetime': 'datetime64',
                    'dropOff_datetime': 'datetime64'
                }
    
    df_type_map = {}
    for c in columns:
        if type_map.get(c, None):
            df_type_map[c] = type_map[c]
    
    return df_type_map

@task(log_prints=True)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""

    if 'tpep_pickup_datetime' in df.columns:
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    elif 'lpep_dropoff_datetime' in df.columns:
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    
    col_types = get_type_map(df.columns)
    df = df.astype(col_types, copy=False)

    print(f"rows: {len(df)}")
    # print(f"columns: {df.dtypes}")

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

    if save_path.endswith("csv.gz"):
        buffer = io.BytesIO()
        out = df.to_csv(buffer, index=False, compression="gzip")
        buffer.seek(0)
    elif save_path.endswith(".parquet"):
        out = df.to_parquet(compression="gzip")
        buffer = io.BytesIO(out)

    gcs_block.upload_from_file_object(buffer, to_path=save_path)

    return


@flow()
def load_trips_data(color:str='green', year:int=2020, months:list=[5], out_format="parquet") -> None:
    """The main ETL function"""

    total_records = 0
    for month in months:
        dataset_fn = f"{color}_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_fn}.csv.gz"

        df = fetch(dataset_url)
        df_clean = transform(df)
        
        # skipping this step
        # path = write_local(df_clean, color, dataset_file)

        bucket_path = f"{color}/{dataset_fn}.{out_format}"
        write_gcs(df_clean, bucket_path)
        total_records += len(df_clean)
        del df, df_clean
        gc.collect()

    print(f"Processes Total of {total_records} records & Saved to GCS.")


if __name__ == "__main__":
    colors = ["fhv"] # green, yellow, fhv
    years = [2019, 2020]
    month = list(range(1, 13))

    for color in colors:
        for year in years:
            load_trips_data(color, year, month, out_format="csv.gz")
