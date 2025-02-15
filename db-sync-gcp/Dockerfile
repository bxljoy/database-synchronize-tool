FROM --platform=linux/amd64 python:3.11-slim

WORKDIR /app

# Copy requirements.txt first (for better layer caching)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all gcp Python files
COPY gcp_*.py .
COPY gcs_sync.py .

# Copy all YAML config files
COPY *.yaml .

# Run the script
CMD ["python", "gcp_main.py"]