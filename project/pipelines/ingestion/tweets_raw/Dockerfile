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
RUN useradd -m -u 1000 appuser
USER appuser

CMD ["python", "-m", "tweets_raw.extract", "--keywords", "PSG OR OM", "--max-results", "100"]
