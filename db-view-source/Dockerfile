FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY sdk/ ./sdk/
RUN pip install --no-cache-dir --timeout 1800 ./sdk/ boto3 requests==2.32.3

COPY fetch.py ./
RUN pip install --no-cache-dir "cbr-pipeline-sdk @ https://github.com/datakaveri/cbr-pipeline-sdk/releases/download/v2.2.0/cbr_pipeline_sdk-2.2.0-py3-none-any.whl"

CMD ["python", "fetch.py"]
