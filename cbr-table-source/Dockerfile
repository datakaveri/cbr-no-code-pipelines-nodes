FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

COPY sdk/ ./sdk/
RUN uv pip install --system --no-cache ./sdk/ boto3 requests==2.32.3

COPY transform.py  storage_v2.py storage_boto3.py ./

CMD ["python", "transform.py"]
