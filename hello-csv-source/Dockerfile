FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir boto3 requests==2.32.3

COPY main.py ./
RUN pip install --no-cache-dir "cbr-pipeline-sdk @ https://github.com/datakaveri/cbr-pipeline-sdk/releases/download/v2.2.0/cbr_pipeline_sdk-2.2.0-py3-none-any.whl"

CMD ["python", "main.py"]
