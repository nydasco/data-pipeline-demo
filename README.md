# data-pipeline-demo
A demonstration of an ELT (Extract, Load, Transform) pipeline using Polars to store data in DeltaTables within local S3 object storage.

Run `docker-compose up` to start MinIO

You will need to create three buckets:
1. bronze
2. silver
3. gold

Run the queries in the following order:
1. get_rates_to_bronze.py
2. transform_rates_to_silver.py
3. present_rates_in_gold.py

The output will be a fact table and a dimension table.