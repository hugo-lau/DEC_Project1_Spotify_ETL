FROM python:3.9-slim-bookworm

# Install ping and curl
RUN apt-get update && apt-get install -y iputils-ping curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY /app .

COPY requirements.txt .

RUN pip install -r requirements.txt

CMD ["python", "-m", "etl_project.pipelines.spotify"]
