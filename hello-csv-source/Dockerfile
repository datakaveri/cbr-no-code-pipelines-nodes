FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir boto3 requests==2.32.3

COPY main.py ./
COPY pipeline_sdk/ ./pipeline_sdk/

CMD ["python", "main.py"]
