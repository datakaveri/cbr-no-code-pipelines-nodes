FROM python:3.11-slim

WORKDIR /app

# Install DuckDB (prebuilt wheel - very fast) and requests (presigned-URL uploads)
RUN pip install --no-cache-dir duckdb requests==2.32.3

# Pre-install httpfs extension (required for S3 access)
# This avoids runtime download which fails in air-gapped k8s environments
RUN python -c "import duckdb; conn = duckdb.connect(); conn.install_extension('httpfs'); conn.close()"

# Copy script
COPY transform.py ./

CMD ["python", "transform.py"]
