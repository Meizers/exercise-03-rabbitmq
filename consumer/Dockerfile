FROM python:3.14-slim AS builder
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.14-slim
# Unbuffered stdout: every event line reaches `docker compose logs consumer` immediately.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY --from=builder /install /usr/local
RUN useradd --create-home --uid 10001 consumer
USER consumer
COPY main.py .
CMD ["python", "main.py"]
