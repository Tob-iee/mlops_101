
# from fileinput import filename

import pickle
import pandas as pd
import boto3
import sys

from prefect import task, flow, get_run_logger
from prefect.context import get_run_context

from dateutil.relativedelta import relativedelta
from datetime import datetime

def read_data(input_file: str, year: int, month: int):
    df = pd.read_parquet(input_file)

    df['duration'] = df.lpep_dropoff_datetime - df.lpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    # categorical = ['PUlocationID', 'DOlocationID']
    # df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')

    df['ride_id'] = f"{year:04d}/{month:02d}_" + df.index.astype('str')

    return df

def prepare_dictionaries(df: pd.DataFrame):

    categorical = ['PULocationID', 'DOLocationID']
    df[categorical] = df[categorical].astype(str)

    dicts = df[categorical].to_dict(orient='records')

    return dicts


def load_model(model_path: str):

    with open(model_path, 'rb') as f_in:
        dv, lr = pickle.load(f_in)

    return dv, lr

def save_results(df: pd.DataFrame, y_pred, output_file):

    df_result = pd.DataFrame()

    df_result['ride_id'] = df['ride_id']
    df_result["output"] = y_pred

    df_result.to_parquet(
    output_file,
    engine='pyarrow',
    compression=None,
    index=False)




@task
def apply_model(input_file: str, model_path, output_file,  year: int, month: int):
    logger = get_run_logger()

    logger.info(f'reading the data from {input_file}...')
    df = read_data(input_file, year, month)
    dicts = prepare_dictionaries(df)

    logger.info(f'loading the model with RUN_ID={model_path}...')
    dv, lr = load_model(model_path)

    logger.info(f'applying the model...')
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)

    logger.info(f'saving the result to {output_file}...')
    save_results(df, y_pred, output_file)

    avg = sum(y_pred/len(y_pred))
    print(f"the mean duration for the  month of {month} is {avg}")

    return output_file


@flow
def duration_pred_pipeline(
    taxi_type: str,
    run_id: str,
    run_date: datetime = None):

    if run_date is None:
        ctx =  get_run_context()
        run_date = ctx.flow_run.expected_start_time

    prev_month = run_date - relativedelta(months=1)
    year = prev_month.year
    month = prev_month.month


    # input_file = f's3://nyc-tlc/trip data/{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet'
    input_file = f'https://nyc-tlc.s3.amazonaws.com/trip+data/{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet'
    output_file = f"s3://mlflow-artifacts-remote-mlops-101/taxi_type={taxi_type}/year={year:04d}/month={month:02d}/{run_id}.parquet"

    apply_model(input_file,
        run_id,
        output_file,
        year,
        month)


def run():
    taxi = sys.argv[1] # "fhv"
    year = int(sys.argv[2]) # 2021
    month = int(sys.argv[3]) #2


    run_id = sys.argv[4] # "model.bin"



    duration_pred_pipeline(
    taxi,
    run_id,
    run_date=datetime(year=year, month=month, day=1))


if __name__ == '__main__':
    run()
