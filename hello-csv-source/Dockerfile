FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir boto3 requests==2.32.3

COPY main.py ./
RUN pip install --no-cache-dir cbr-pipeline-sdk==2.2.0

CMD ["python", "main.py"]
