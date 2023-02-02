#!/usr/bin/env python
# coding: utf-8
import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector


# @task(tags=["extract"])
def extract_data(url: str, csv_name):
    
    # download the file
    os.system(f"wget -nc {url} -O {csv_name}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    return df_iter


@task(log_prints=True)
def transform_data(df):
    if 'tpep_pickup_datetime' in df.columns:
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    elif 'lpep_dropoff_datetime' in df.columns:
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)


    if 'passenger_count' in df.columns:
        print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
        df = df[df['passenger_count'] != 0].reset_index(drop=True)
        print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")

    return df


@task(log_prints=True, retries=3)
def load_data(conn, df, table_name, save_header=False):
    with conn.get_connection(begin=False) as engine:
        if save_header:
            df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')


@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
    print(f"Logging table name from Subflow: {table_name}")


@flow(name="ingest_from_web_to_pg", log_prints=True)
def main_flow(csv_url:str, table_name:str):

    log_subflow(table_name)

    # store csv name
    # # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if csv_url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    # get data iterator (chunks)
    raw_data_iter = extract_data(csv_url, csv_name)
    # temp_df = next(raw_data_iter)
    # print(temp_df.shape)

    # get connection_block
    conn_block = SqlAlchemyConnector.load("ny-taxi-postgress-connection") 

    chunk_num = 0
    # repeat the steps for full data chunks
    for raw_chunk in raw_data_iter:
        save_header = False
        if chunk_num == 0:
            save_header = True

        t_start = time()
        clean_df = transform_data(raw_chunk)
        _ = load_data(conn_block, clean_df, table_name, save_header)
        t_end = time()

        chunk_num += 1
        print('inserted another chunk, took %.3f second' % (t_end - t_start))
        
    print(f"Finished ingesting data into the postgres database. total chunks = {chunk_num}")
    os.system(f"rm -rf {csv_name}")
    


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    # url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    # add connection url to SqlAlchemyConnector block in the UI (give name as ny-taxi-postgress-connection)
    # postgres_url = f'postgresql://root:root@pgdatabase:5432/ny_taxi'
    main_flow(args.url, args.table_name)

