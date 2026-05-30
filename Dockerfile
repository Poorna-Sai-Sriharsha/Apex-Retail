FROM python:3.11-slim

# System dependencies for OpenCV and PyTorch
RUN apt-get update && apt-get install -y \
    libglib2.0-0 \
    libsm6 \
    libxrender1 \
    libxext6 \
    libgl1-mesa-glx \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir fastapi==0.111.0 uvicorn[standard]==0.29.0 \
    pydantic==2.7.1 pydantic-settings==2.2.1 \
    sqlalchemy==2.0.30 aiosqlite==0.20.0 asyncpg==0.29.0 \
    structlog==24.2.0 python-json-logger==2.0.7 \
    httpx==0.27.0 numpy==1.26.4 scipy==1.13.0 \
    rich==13.7.1 python-multipart==0.0.9 python-dotenv==1.0.1

# Copy application code
COPY app/ ./app/
COPY pipeline/store_layouts/ ./pipeline/store_layouts/
COPY .env.example .env

# Create output directory
RUN mkdir -p /app/pipeline/output

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]
