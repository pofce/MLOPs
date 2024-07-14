# Sentiment Analysis Project

This repository contains the components required to set up and run the sentiment analysis model using Docker containers managed with Docker Compose.

## Requirements

- Python 3.11
- Docker
- Docker Compose

## Installation

1. **Clone the repository**:
   Ensure that you have cloned the repository to your local machine.

2. **Navigate to the project directory**:
   Change directory into the project root, where the `docker-compose.yml` and Dockerfiles are located.

   ```bash
   cd path/to/Sentiment_Analysis
   ```

3. **Start Airflow and MinIO**:
   Use Docker Compose to start the Airflow and MinIO.

   ```bash
   docker-compose -f airflow/docker-compose.yaml up -d
   ```

4. **Build the model API Docker image**:
   This will create a Docker image for the sentiment analysis model API from the Dockerfile located in the `app` directory.

   ```bash
   docker build -t model_api app
   ```

5. **Run the model API**:
   Start the model API container. This will serve the model on port 5050 of your local machine.

   ```bash
   docker run --name serving_model -p 5050:5050 -d model_api
   ```

## Usage

The sentiment analysis API is accessible at `http://localhost:5050/predict`. You can send data to this endpoint to get predictions.

```bash
curl -X POST http://localhost:5050/predict \
     -H "Content-Type: application/json" \
     -d '{"sentence": "I love learning about AI!"}'
```

## Stopping the Project

To stop all running containers related to this project:

```bash
docker-compose -f airflow/docker-compose.yml down
docker stop serving_model
```
