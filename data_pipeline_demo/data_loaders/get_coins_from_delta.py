import polars as pl
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    """
    Get the id column from the most recent version of the coins table in Delta
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

    df = pl.read_delta(
        source = uri,
        storage_options = storage_options
    )

    return df['id']

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
