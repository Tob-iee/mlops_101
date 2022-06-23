FROM agrigorev/zoomcamp-model:mlops-3.9.7-slim

COPY [ "Pipfile.lock", "Pipfile", "./" ]


RUN pip install -U pip

RUN pip install pipenv

RUN pipenv install --system --deploy

COPY [ "infer.py", "model.bin", "./" ]

COPY output /app/output

CMD python infer.py fhv 2021 4 model.bin mlflow-artifacts-remote-mlops-101