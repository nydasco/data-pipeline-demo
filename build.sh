#! /bin/bash

# Build Airflow image
#docker build -t data-pipeline-demo . -f build/elt/DockerFile

# Build Plotly image
docker build -t data-viz-demo . -f build/viz/DockerFile