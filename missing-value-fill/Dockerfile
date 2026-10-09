FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt main.py ./
COPY pipeline_sdk/ ./pipeline_sdk/

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]
