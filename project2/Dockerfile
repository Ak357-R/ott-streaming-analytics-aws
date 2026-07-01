# OTT Recommendation API - Docker Container
# Base image: Python 3.9 slim (minimal size)
# Server: Gunicorn with 3 workers
# Port: 5000
#
# Build:  docker build -t ott-recommendation-api .
# Run:    docker run -d -p 8080:5000 ott-recommendation-api
# Test:   curl http://localhost:8080/health

FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy requirements first (layer caching - faster rebuilds)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY recommendation_api.py .

# Expose port
EXPOSE 5000

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:5000/health || exit 1

# Run with Gunicorn (production server)
# 3 workers = handles concurrent requests
CMD ["gunicorn", "--workers", "3", "--bind", "0.0.0.0:5000", "--timeout", "60", "recommendation_api:app"]
