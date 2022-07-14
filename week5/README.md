## Machine Learning for Streaming

* Scenario
* Creating the role
* Create a Lambda function, test it
* Create a Kinesis stream
* Connect the function to the stream
* Send the records

Links

* [Tutorial: Using Amazon Lambda with Amazon Kinesis](https://docs.amazonaws.cn/en_us/lambda/latest/dg/with-kinesis-example.html)

## Code snippets

Sending data:

```bash
KINESIS_STREAM_INPUT=ride_events
aws kinesis put-record \
    --stream-name ${KINESIS_STREAM_INPUT} \
    --partition-key 1 \
    --data "Hello, this is a test." \
    --cli-binary-format raw-in-base64-out
```

Decoding base64

```python
base64.b64decode(data_encoded).decode('utf-8')
```

Record example

```json
{
    "ride": {
        "PULocationID": 130,
        "DOLocationID": 205
    },
    "ride_id": 123
}
```

Sending this record

```bash
aws kinesis put-record \
    --stream-name ${KINESIS_STREAM_INPUT} \
    --partition-key 1 \
    --data '{
        "ride": {
            "PULocationID": 130,
            "DOLocationID": 205
        },
        "ride_id": 156
    }' \
    --cli-binary-format raw-in-base64-out
```

Test event:

```json
{
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
```


Reading from the stream

```bash
KINESIS_STREAM_OUTPUT='ride_predictions'
SHARD='shardId-000000000000'

SHARD_ITERATOR=$(aws kinesis \
    get-shard-iterator \
        --shard-id ${SHARD} \
        --shard-iterator-type TRIM_HORIZON \
        --stream-name ${KINESIS_STREAM_OUTPUT} \
        --query 'ShardIterator' \
)

RESULT=$(aws kinesis get-records --shard-iterator $SHARD_ITERATOR)

echo ${RESULT} | jq -r '.Records[0].Data' | base64 --decode
```

Running the test

```bash
export PREDICTIONS_STREAM_NAME="ride_predictions"
export RUN_ID="your model run id"
export TEST_RUN="True"

python test.py
```


Putting everything to Docker

```bash
docker build -t stream-model-duration:v1 .

docker run -it --rm \
    -p 8080:8080 \
    -e PREDICTIONS_STREAM_NAME="ride_predictions" \
    -e RUN_ID="8c01d893d5b0439e9b1d48cafbb2d96b" \
    -e TEST_RUN="True" \
    -e AWS_DEFAULT_REGION="us-east-1" \
    stream-model-duration:v1
```

To use AWS CLI, you may need to set the env variables:

export AWS_ACCESS_KEY_ID="your AWS_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY= "your AWS_SECRET_ACCESS_KEY"
export AWS_DEFAULT_REGION=us-east-1

```bash
docker run -it --rm \
    -p 8080:8080 \
    -e PREDICTIONS_STREAM_NAME="ride_predictions" \
    -e RUN_ID="8c01d893d5b0439e9b1d48cafbb2d96b" \
    -e TEST_RUN="True" \
    -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
    -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
    -e AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION}" \
    stream-model-duration:v1
```

Alternatively, you can mount the `.aws` folder with your credentials to the `.aws` folder in the container:

```bash
docker run -it --rm \
    -p 8080:8080 \
    -e PREDICTIONS_STREAM_NAME="ride_predictions" \
    -e RUN_ID="8c01d893d5b0439e9b1d48cafbb2d96b" \
    -e TEST_RUN="True" \
    -v c:/Users/alexe/.aws:/root/.aws \
    stream-model-duration:v1
```

URL for testing:

* http://localhost:8080/2015-03-31/functions/function/invocations


Creating an ECR repo

```bash
aws ecr create-repository --repository-name duration-model
```

Logging in

```bash
$(aws ecr get-login --no-include-email)
```

Pushing

```bash
REMOTE_URI="823667002813.dkr.ecr.us-east-1.amazonaws.com/duration-model"
REMOTE_TAG="v1"
REMOTE_IMAGE=${REMOTE_URI}:${REMOTE_TAG}

LOCAL_IMAGE="stream-model-duration:v1"
docker tag ${LOCAL_IMAGE} ${REMOTE_IMAGE}
docker push ${REMOTE_IMAGE}
```