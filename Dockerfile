# Dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# Render sets PORT automatically; uvicorn listens on 0.0.0.0
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "10000"]
