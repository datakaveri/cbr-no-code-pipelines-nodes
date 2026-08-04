FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY sdk/ ./sdk/
RUN pip install --no-cache-dir --timeout 1800 ./sdk/ boto3 requests==2.32.3

COPY fetch.py ./

CMD ["python", "fetch.py"]
