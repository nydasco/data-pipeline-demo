#! /bin/bash

# Build Airflow image
docker build -t data-pipeline-demo . -f ./DockerFile

docker-compose up
