#!/bin/bash

cd build/airflow
docker build -t data-pipeline-airflow-demo . -f DockerFile

cd ../jupyter
docker build -t data-pipeline-jupyter-demo . -f DockerFile