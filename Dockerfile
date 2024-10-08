# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the current directory contents into the container
COPY . .

# Install system dependencies for SQLite (required for pysqlite3) and any additional dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Define environment variables (can be overridden when running the container)
ENV DB_FILE=torrents.db
ENV ZURGINFODIR=/mnt/zurginfo
ENV RCLONE_REMOTE_PATH=/mnt/webdav

# Make sure the working directories exist
RUN mkdir -p /mnt/zurginfo /mnt/webdav

# Run the application
CMD ["python", "your_script.py"]
