# Stage 1: Build the Python application
FROM python:3.8-slim-bullseye AS builder

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

CMD ["python", "main.py"]
