```
docker build -t batch-model-duration:v2 .
```
```
docker run -it --rm \
    batch-model-duration:v2
```
```
aws --endpoint-url=http://localhost:4566 s3 mb s3://nyc-duration
```