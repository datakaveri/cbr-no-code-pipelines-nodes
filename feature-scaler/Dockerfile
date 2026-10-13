FROM python:3.11-slim

# Install dependencies
RUN pip install --no-cache-dir \
    pandas==2.2.3 \
    pyarrow==19.0.1 \
    scikit-learn==1.6.1 \
    requests==2.32.3

WORKDIR /app
COPY transform.py ./
RUN pip install --no-cache-dir "cbr-pipeline-sdk @ https://github.com/datakaveri/cbr-pipeline-sdk/releases/download/v2.2.0/cbr_pipeline_sdk-2.2.0-py3-none-any.whl"

CMD ["python", "transform.py"]
