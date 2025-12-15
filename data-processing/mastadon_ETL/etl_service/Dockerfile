# Use lightweight Python image
FROM python:3.10-slim

# Create working directory
WORKDIR /app

# Install system dependencies (for psycopg2)
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (better caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the ETL script
COPY etl_script.py .

# Ensure .env can be read (compose will pass it into the container)
ENV PYTHONUNBUFFERED=1

# Default command: run the ETL job
CMD ["python", "etl_script.py"]
