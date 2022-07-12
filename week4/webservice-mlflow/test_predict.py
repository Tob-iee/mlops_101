import requests

ride = {
    "PULocationID": 10,
    "DOLocationID": 40,
}

url = 'http://localhost:9696/predict'
response = requests.post(url, json=ride)
print(response.json())
