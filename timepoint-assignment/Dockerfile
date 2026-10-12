FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt main.py ./
RUN pip install --no-cache-dir "cbr-pipeline-sdk @ https://github.com/datakaveri/cbr-pipeline-sdk/releases/download/v2.2.0/cbr_pipeline_sdk-2.2.0-py3-none-any.whl"

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]
