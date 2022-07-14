import re
import requests

event = {
    "Records": [
        {
            "kinesis": {
                "kinesisSchemaVersion": "1.0",
                "partitionKey": "1",
                "sequenceNumber": "49631312568382935461090870552880796788984569821829005314",
                "data": "ewogICAgICAgICJyaWRlIjogewogICAgICAgICAgICAiUFVMb2NhdGlvbklEIjogMTMwLAogICAgICAgICAgICAiRE9Mb2NhdGlvbklEIjogMjA1CiAgICAgICAgfSwKICAgICAgICAicmlkZV9pZCI6IDE1NgogICAgfQ==",
                "approximateArrivalTimestamp": 1657625319.345
            },
            "eventSource": "aws:kinesis",
            "eventVersion": "1.0",
            "eventID": "shardId-000000000000:49631312568382935461090870552880796788984569821829005314",
            "eventName": "aws:kinesis:record",
            "invokeIdentityArn": "arn:aws:iam::823667002813:role/lambda-kinesis-role",
            "awsRegion": "us-east-1",
            "eventSourceARN": "arn:aws:kinesis:us-east-1:823667002813:stream/ride_events"
        }
    ]
}


url = "http://localhost:8080/2015-03-31/functions/function/invocations"
response = requests.post(url, json=event)
print(response.json())
