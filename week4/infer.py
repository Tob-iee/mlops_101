
from fileinput import filename
import os
import pathlib
import pickle
import pandas as pd
import boto3
import sys
import math
import pprint


def read_data(filename, year, month):
    df = pd.read_parquet(filename)

    df['duration'] = df.dropOff_datetime - df.pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    categorical = ['PUlocationID', 'DOlocationID']
    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')

    df['ride_id'] = f"{year:04d}/{month:02d}_" + df.index.astype('str')

    return df


def load_model(model_path):

    with open(model_path, 'rb') as f_in:
        dv, lr = pickle.load(f_in)

    return dv, lr


def apply_model(input_file, model_path, output_file, year, month, bucket_target):

    dv, lr = load_model(model_path)
    df = read_data(input_file, year, month)

    categorical = ['PUlocationID', 'DOlocationID']

    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)

    df_result = pd.DataFrame()

    df_result['ride_id'] = df['ride_id']
    df_result["output"] = y_pred

    output_file_path = f"output/{output_file}"

    df_result.to_parquet(
    output_file_path,
    engine='pyarrow',
    compression=None,
    index=False
    )

    avg = sum(y_pred/len(y_pred))
    print(f"the mean duration for the  month of {month} is {avg}")

    upload_predictions_to_s3(bucket_target, output_file)




def upload_predictions_to_s3(bucket_target, output_file):
    """
    Uploads file to S3 bucket using S3 client object
    :return: None
    """
    s3 = boto3.client("s3")
    object_name = "output/"+output_file
    file_name = f"output/{output_file}"
    # s3.put_object(Bucket=bucket_target, Key=f"output")
    
    print("s3 upload Uploading .....")
    response = s3.upload_file(file_name, bucket_target, object_name)
    print("Done!!!")  # prints None



def run():
    taxi = sys.argv[1] # "fhv"
    year = int(sys.argv[2]) # 2021
    month = int(sys.argv[3]) #2

    input_file = f'https://nyc-tlc.s3.amazonaws.com/trip+data/{taxi}_tripdata_{year:04d}-{month:02d}.parquet'
    output_file = f"fhv_tripdata_{year:04d}-{month:02d}.parquet"

    model_path = sys.argv[4] # "model.bin"
    bucket_target = sys.argv[5]  # mlflow-artifacts-remote-mlops-101


    apply_model(input_file,
        model_path,
        output_file,
        year,
        month,
        bucket_target)

if __name__ == '__main__':
    run()
