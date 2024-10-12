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

# Stage 2: Runtime image
FROM python:3.8-slim-bullseye

# Set working directory
WORKDIR /app

# Copy the installed packages and application code from the builder stage
COPY --from=builder /usr/local/lib/python3.8/ /usr/local/lib/python3.8/
COPY --from=builder /app /app

# Expose any necessary ports (if your application listens on a port)
# EXPOSE 8000

# Set environment variables if needed
# ENV VARIABLE=value

# Default command to run the application in production
CMD ["python", "main.py"]
