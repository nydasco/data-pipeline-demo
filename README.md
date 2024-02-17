# data-pipeline-demo
A demonstration of an ELT (Extract, Load, Transform) pipeline using Polars to store data in DeltaTables within local S3 object storage (MinIO), orchestrated with Airflow.

In order to run this demonstration:
1. You'll need to get yourself an API key, which you can access from here: `https://www.alphavantage.co/support/#api-key`. Simply update the `api_key` value in `pipelines/params.py`.
2. This build of Airflow and Jupyter have custom dependecies. As such, you need to build the custom images the first time you run this repo. A `build.sh` file has been provided that can be run. The details of what is being created can be seen in the `build/` path.
3. Run `docker-compose up` to start MinIO, Airflow & Jupyter Notebook

You can view the contents of the buckets as they're populated at http://localhost:9001 using username: `minio` password: `minio123`.
You can access Airflow at http://localhost:8080 using username: `airflow` password: `airflow`. From here you can run the DAG and populate the buckets.
You can access Jupyter at http://localhost:8888?token=easy. The output of the pipeline will be a fact table and dimension tables in the `gold` bucket. You can see this by opening the `connect_to_gold.ipynb` file within the `work` folder in Jupyter.

Now that you have both tables loaded into Polars DataFrames within Jupyter, feel free to have a play around with them.