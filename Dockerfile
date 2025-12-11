FROM python:3.11-slim

WORKDIR /app

ENV REFRESHED_AT=2025-12-11_v3

# Install system dependencies (gcc for cffi/grpc)
RUN apt-get update && apt-get install -y gcc

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip list # Debug: Check installed packages

COPY . .

# Default command (can be overridden by Cloud Run Job args)
CMD ["python", "app.py"]
