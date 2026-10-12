FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt main.py ./
RUN pip install --no-cache-dir cbr-pipeline-sdk==2.2.0

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]
