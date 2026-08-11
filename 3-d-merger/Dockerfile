FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt merge_stl.py  storage_v2.py storage_boto3.py ./

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "merge_stl.py"]
