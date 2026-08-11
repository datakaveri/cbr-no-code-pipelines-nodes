FROM python:3.11-slim

WORKDIR /app

# Install DuckDB (prebuilt wheel - very fast) and requests (presigned-URL uploads)
RUN pip install --no-cache-dir duckdb requests==2.32.3

# Pre-install httpfs extension (required for S3 access)
# This avoids runtime download which fails in air-gapped k8s environments
RUN python -c "import duckdb; conn = duckdb.connect(); conn.install_extension('httpfs'); conn.close()"

# Install clustering dependencies
RUN pip install --no-cache-dir \
    pandas==2.2.3 \
    pyarrow==19.0.1 \
    scikit-learn==1.6.1 \
    numpy==2.2.4

COPY transform.py  storage_v2.py ./

CMD ["python", "transform.py"]
