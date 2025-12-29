# Dockerfile for the data streamer service
FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY pyproject.toml uv.lock ./

# Install uv and sync dependencies from pyproject.toml
RUN pip install --no-cache-dir uv && \
    uv sync --frozen

# Copy the streaming scripts
COPY stream_service.py .

# Note: For Cloud Run, we use run_streamer_cloudrun.py which provides
# HTTP health checks while running the streamer in the background

# Run the Cloud Run wrapper (provides health checks on $PORT)
CMD ["uv", "run", "python", "-u", "stream_service.py"]
