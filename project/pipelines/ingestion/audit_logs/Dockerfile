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

# create a non-root user and switch to that user for better security practices.
RUN useradd -m -u 1000 appuser
USER appuser

# Run the extract module when the container starts.
#CMD ["python", "-m", "extract"]
