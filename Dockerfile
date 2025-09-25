FROM python:3.10.8-slim-buster

# Set working directory
WORKDIR /usr/src/app

# Install system dependencies (git, gcc, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    build-essential \
 && rm -rf /var/lib/apt/lists/*

# Copy requirements first (for caching)
COPY requirements.txt .

# Upgrade pip and install Python dependencies
RUN pip3 install --no-cache-dir -U pip \
 && pip3 install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Run your app
CMD ["bash", "start.sh"]
