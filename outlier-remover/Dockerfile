FROM python:3.11-slim

RUN pip install --no-cache-dir \
    pandas==2.2.3 \
    pyarrow==19.0.1 \
    scikit-learn==1.6.1 \
    numpy==2.2.4 \
    boto3==1.37.38 \
    requests==2.32.3

WORKDIR /app
COPY transform.py .

CMD ["python", "transform.py"]
