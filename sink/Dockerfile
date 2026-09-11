FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY iceberg_sink.py .
CMD ["python", "-u", "iceberg_sink.py"]
