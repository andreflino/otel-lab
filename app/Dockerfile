FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
 && opentelemetry-bootstrap -a install
COPY *.py .
# Zero-code instrumentation: Flask, requests, redis, pika and logging are instrumented automatically.
ENTRYPOINT ["opentelemetry-instrument", "python"]
