FROM python:3.11-slim

WORKDIR /app

ENV REFRESHED_AT=2025-12-11_v2

# Install system dependencies if needed (e.g. for building some python packages)
# RUN apt-get update && apt-get install -y gcc

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default command (can be overridden by Cloud Run Job args)
CMD ["python", "ingest_data.py"]
