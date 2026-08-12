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

COPY tweets_raw ./tweets_raw

ENV PYTHONPATH=/app:/app/tweets_raw

# create a non-root user and switch to that user for better security practices.
# /app/logs is owned by appuser so a fresh shared named volume mounted there
# inherits appuser ownership (Docker copies the image dir ownership).
RUN useradd -m -u 1000 appuser \
    && mkdir -p /app/logs \
    && chown -R appuser:appuser /app/logs
USER appuser

CMD ["python", "-m", "tweets_raw.extract", "--keywords", "PSG OR OM", "--max-results", "100"]
