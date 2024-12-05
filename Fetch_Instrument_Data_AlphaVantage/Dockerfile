FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /financedb

# Copy project files
COPY . /financedb

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5000
