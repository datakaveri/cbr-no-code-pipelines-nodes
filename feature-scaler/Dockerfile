FROM python:3.11-slim

# Install dependencies
RUN pip install --no-cache-dir \
    pandas==2.2.3 \
    pyarrow==19.0.1 \
    scikit-learn==1.6.1 \
    requests==2.32.3

WORKDIR /app
COPY transform.py  storage_v2.py ./

CMD ["python", "transform.py"]
