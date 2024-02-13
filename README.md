# data-pipeline-demo
A demonstration of an ELT (Extract, Load, Transform) pipeline using Polars to store data in DeltaTables within local S3 object storage.

In order to run this demonstration:
1. You'll need to get yourself an API key, which you can access from here: `https://www.alphavantage.co/support/#api-key`. Simply update the `api_key` value in `dags/params.py`.
2. This build of Airflow has custom dependecies identified in `requirements.txt`. As such, you need to build the custom image using `docker build -t data-pipeline-demo . -f ./DockerFile`.
3. Run `docker-compose up` to start MinIO and Airflow

You can view the contents of the buckets as they're populated at http://localhost:9001 using username: `minio` password: `minio123`.
You can access Airflow at http://localhost:8080 using username: `airflow` password: `airflow`.

The output will be a fact table and a dimension table in the `gold` bucket.
