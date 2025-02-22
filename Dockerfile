# Use an official Python runtime as a base image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y python3-distutils python3-setuptools python3-wheel build-essential && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Upgrade pip, setuptools, and wheel
RUN pip install --upgrade pip setuptools wheel

# Copy the requirements.txt file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire application into the container
COPY . .

# Set environment variables to ensure the app works inside Docker
ENV PYTHONUNBUFFERED=1

# Expose port 8080 to the host
EXPOSE 8080

# Set the command to run your app using gunicorn for production
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "app:app"]