# Use an official Python runtime as a base image
FROM python:3.11-slim-bullseye

# Set environment variables for Python
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies required for MySQL client
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    apt-transport-https \
    ca-certificates \
    software-properties-common \
    gnupg \
    gcc \
    libffi-dev \
    libicu67 \
    libssl-dev \
    libc-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set the working directory inside the container
WORKDIR /app

# Install dependencies
COPY requirements.txt /app/
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# Copy the rest of the codebase into the container
COPY . /app/

# Set default command
CMD ["python3", "psyduckv2.py"]
