# Use a slim Python base image
FROM python:3.9-slim-bookworm

# Install dependencies: ping and curl
RUN apt-get update && apt-get install -y iputils-ping curl && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app2

# Copy only requirements first to leverage Docker cache
COPY requirements.txt . 

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application code
COPY . .  

# Command to run your application
CMD ["python extract_load.py"]
