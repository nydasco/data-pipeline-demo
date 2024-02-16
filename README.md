# data-pipeline-demo
A demonstration of an ELT (Extract, Load, Transform) pipeline using Polars to store data in DeltaTables within local S3 object storage.

In order to run this demonstration:
1. You'll need to get yourself an API key, which you can access from here: `https://www.alphavantage.co/support/#api-key`. Simply update the `api_key` value in `dags/params.py`.
2. This build of Airflow has custom dependecies identified in `requirements.txt`. As such, you need to build the custom image using `docker build -t data-pipeline-demo . -f ./DockerFile`.
3. Run `docker-compose up` to start MinIO, Airflow & Jupyter Notebook

You can view the contents of the buckets as they're populated at http://localhost:9001 using username: `minio` password: `minio123`.
You can access Airflow at http://localhost:8080 using username: `airflow` password: `airflow`. You can access Jupyter at http://localhost:8888?token=easy

The output will be a fact table and a dimension table in the `gold` bucket.

To see the results in Jupyter, you can copy/paste the below into the notebook:

```
%pip install polars
%pip install deltalake
```

You will then have to restart the kernel with the restart button at the top.

Then:
```
import polars as pl

storage_options = {
    "AWS_ACCESS_KEY_ID": "minio",
    "AWS_SECRET_ACCESS_KEY": "minio123",
    "AWS_REGION": "us-east-1",
    "AWS_ENDPOINT_URL": "http://10.5.0.5:9000",
    "AWS_ALLOW_HTTP": "TRUE",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "TRUE",
}
```

```
dim_currency_uri = f"s3://gold/dim_currency"

dim_currency = pl.read_delta(
        dim_currency_uri,
        storage_options = storage_options
     )

print(dim_currency)
```

```
fct_rates_uri = f"s3://gold/fct_rates"

fct_rates = pl.read_delta(
        fct_rates_uri,
        storage_options = storage_options
     )

print(fct_rates)
```

Now that you have both tables loaded into Polars DataFrames within Jupyter, feel free to have a play around with them.