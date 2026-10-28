FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt main.py ./
RUN pip install --no-cache-dir "cbr-pipeline-sdk @ https://github.com/datakaveri/cbr-pipeline-sdk/releases/download/v2.2.0/cbr_pipeline_sdk-2.2.0-py3-none-any.whl"

RUN pip install --no-cache-dir -r requirements.txt

# DuckDB's spatial add-on reads GeoPackage files. It is installed now, while the image is built,
# so a running node never has to download anything.
RUN python -c "import duckdb; c = duckdb.connect(); c.execute(\"SET extension_directory='/opt/duckdb_extensions'\"); c.execute('INSTALL spatial')" \
    && chmod -R a+rX /opt/duckdb_extensions

CMD ["python", "main.py"]
