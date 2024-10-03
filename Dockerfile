FROM python:3.9-slim-bookworm

# Install ping and curl
RUN apt-get update && apt-get install -y iputils-ping curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY /app .

#ENV CLIENT_ID="bfaf8650fa0a49fdbffc0ef34701d693"
#ENV CLIENT_SECRET="d06a4f8b11a246d38e9c5fc1b326b8fb"

#ENV DB_USERNAME="postgres"
#ENV DB_PASSWORD="postgres"
#ENV SERVER_NAME="localhost"
#ENV PORT="5432"
#ENV DATABASE_NAME="spotify"

COPY requirements.txt .

RUN pip install -r requirements.txt

CMD ["python", "-m", "etl_project.pipelines.spotify"]
