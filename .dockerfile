FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
RUN pip install --no-cache-dir -r prefect prefect-snowflake snowflake-connector-python snowflake-sqlalchemy requests pandas pyarrow pytz orjson tenacity

# Copy flow code
COPY flows/ ./flows/
COPY sql/ ./sql/

# Set Python path
ENV PYTHONPATH=/app

# Prefect will inject the flow run command
