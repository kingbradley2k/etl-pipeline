FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Compose supplies the long-running scheduler command. This default enables a
# one-off ETL run when the image is started outside Compose.
CMD ["python", "-m", "src.pipeline"]