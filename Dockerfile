# Dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY Telegram_Bot.py .

# Render sets PORT automatically; default to 10000 for local container runs.
CMD ["sh", "-c", "uvicorn Telegram_Bot:app --host 0.0.0.0 --port ${PORT:-10000}"]
