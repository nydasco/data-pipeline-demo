# data-pipeline-demo
A demonstration of an ELT (Extract, Load, Transform) pipeline using Polars to store data in DeltaTables within local S3 object storage.

Run `docker-compose up` to start MinIO and Airflow

You can view the contents of the buckets as they're populated at http://localhost:9001.

You can access Airflow at http://localhost:8080 using username: `airflow` password: `airflow`.

The output will be a fact table and a dimension table.