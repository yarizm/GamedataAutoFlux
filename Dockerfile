FROM python:3.12-slim

# Install system dependencies required for Playwright (Chromium) and project compilation
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    libnss3 \
    libxss1 \
    libasound2 \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    libgbm-dev \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirement files first to leverage Docker cache
COPY pyproject.toml ./

# Install the project and its dependencies
RUN pip install --no-cache-dir -e .

# Install Playwright Chromium browser and its remaining dependencies
RUN playwright install chromium --with-deps

# Copy the rest of the application code
COPY . .

# Ensure data directory exists
RUN mkdir -p data

# Expose port
EXPOSE 8000

# Start the application
CMD ["autoflux"]
