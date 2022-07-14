import os
import json
import boto3
import  base64

import mlflow
from flask import Flask, request, jsonify

kinesis_client = boto3.client("kinesis")

RUN_ID = os.getenv('RUN_ID')

mlflow.set_tracking_uri("http://127.0.0.1:5000")

RUN_ID = "8c01d893d5b0439e9b1d48cafbb2d96b"
logged_model = f's3://mlflow-artifacts-remote-mlops-101/artifacts/1/{RUN_ID}/artifacts/model-artifacts'
# logged_model = f'runs:/{RUN_ID}/model-artifacts'

# mlflow.pyfunc.get_model_dependencies(model_uri)
model = mlflow.pyfunc.load_model(logged_model)

TEST_RUN = os.getenv('DRY_RUN', 'False') == "True"

PREDICTIONS_STREAM_NAME = os.getenv("PREDICTIONS_STREAM_NAME", "ride_predictions")

def prepare_features(ride):
    features = {}
    features['PU_DO'] = '%s_%s' % (ride['PULocationID'], ride['DOLocationID'])
    return features


def predict(features):
    preds = model.predict(features)
    return float(preds[0])


def lambda_handler(event, context):
    print(json.dumps(event))

    prediction_events = []

    for record in event["Records"]:
        encoded_data = record["kinesis"]["data"]
        decoded_data = base64.b64decode(encoded_data).decode("utf-8")
        ride_event = json.loads(decoded_data)

        # print(ride_event)

        ride = ride_event["ride"]
        ride_id = ride_event["ride_id"]

        features = prepare_features(ride)
        prediction = predict(features)

        prediction_event = {
            'model': "ride_duration_prediction_model",
            'version': "123",
            'prediction': {
                'ride_duration': prediction,
                'ride_id': ride_id
            }
        }

        if not TEST_RUN:
            kinesis_client.put_record(
                StreamName=PREDICTIONS_STREAM_NAME,
                Data=json.dumps(prediction_event),
                PartitionKey=str(ride_id)
            )


        prediction_events.append(prediction_event)


    return {
        "predictions" : prediction_events
    }