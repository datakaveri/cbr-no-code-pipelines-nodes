FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir boto3 requests==2.32.3

COPY merge.py  storage_v2.py storage_boto3.py ./

CMD ["python", "merge.py"]
