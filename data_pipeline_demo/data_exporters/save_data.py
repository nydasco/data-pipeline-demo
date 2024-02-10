import pandas as pd
#from deltalake.writer import write_deltalake
from deltalake import write_deltalake
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(df, *args, **kwargs):
    """
    Export data to a Delta Table in a local S3
    """
    storage_options = {
        "AWS_ACCESS_KEY_ID": "CTvEpVXPKyOr2wwBWoHL",
        "AWS_SECRET_ACCESS_KEY":"mTFa0ly4KXREkI1Kd2ngUsbNOpO19CiMlzQxTnQ3",
        "AWS_REGION": "us-east-1",
        "AWS_ENDPOINT_URL": "http://172.20.0.2:9000",
        "AWS_STORAGE_ALLOW_HTTP": "TRUE",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "TRUE",
    }

    uri = 's3://s3storage/coins'
    
    df.write_delta(
        uri,
        mode='append',
        overwrite_schema=True,
        storage_options=storage_options,
    )
    
    print("Success")
