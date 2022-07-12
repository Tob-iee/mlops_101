import os
import pickle

import mlflow
from flask import Flask, request, jsonify

# RUN_ID = os.getenv('RUN_ID')

mlflow.set_tracking_uri("http://127.0.0.1:5000")

RUN_ID = "8c01d893d5b0439e9b1d48cafbb2d96b"
logged_model = f's3://mlflow-artifacts-remote-mlops-101/artifacts/1/{RUN_ID}/artifacts/model-artifacts'
# logged_model = f'runs:/{RUN_ID}/model-artifacts'

# mlflow.pyfunc.get_model_dependencies(model_uri)
model = mlflow.pyfunc.load_model(logged_model)

def prepare_features(ride):
    features = {}
    features['PULocationID'] = ride['PULocationID']
    features['DOLocationID'] = ride['DOLocationID']
    return features


def predict(features):
    preds = model.predict(features)
    return float(preds[0])


app = Flask('duration-prediction')


@app.route('/predict', methods=['POST'])
def predict_endpoint():
    ride = request.get_json()

    features = prepare_features(ride)
    pred = predict(features)

    result = {
        'duration': pred,
        'model_version': RUN_ID
    }

    return jsonify(result)

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=9696)