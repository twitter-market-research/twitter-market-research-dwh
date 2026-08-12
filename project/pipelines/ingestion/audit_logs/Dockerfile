FROM python:3.11-slim

WORKDIR /app

# Install netcat for health checks and clean up apt cache to reduce image size.
RUN apt-get update && \
    apt-get install -y netcat-openbsd && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy the requirements file and install dependencies.
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY audit_logs  ./audit_logs

ENV PYTHONPATH=/app:/app/audit_logs

# create a non-root user and switch to that user for better security practices.
# /app/logs is owned by appuser so the shared named volume mounted there is
# readable/writable by appuser (same uid as the tweets-raw-producer).
RUN useradd -m -u 1000 appuser \
    && mkdir -p /app/logs \
    && chown -R appuser:appuser /app/logs
USER appuser

# Run the audit-log shipper when the container starts.
CMD ["python", "-m", "audit_logs.extract"]
