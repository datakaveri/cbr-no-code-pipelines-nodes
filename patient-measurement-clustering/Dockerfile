FROM ghcr.io/astral-sh/uv:latest AS uv

FROM python:3.11-slim

WORKDIR /app

COPY --from=uv /uv /usr/local/bin/uv
COPY requirements.txt clustering.py  storage_v2.py ./

RUN uv pip install --system --no-cache -r requirements.txt

# Pre-install httpfs extension (required for S3 - avoids runtime download in k8s)
RUN python -c "import duckdb; conn = duckdb.connect(); conn.install_extension('httpfs'); conn.close()"

CMD ["python", "clustering.py"]
