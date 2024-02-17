#!/usr/bin/env python

import polars as pl

class DeltaS3:
    """
    A class for reading and writing Delta tables from/to S3.
    """

    def __init__(self):
        self.storage_options = {
            "AWS_ACCESS_KEY_ID": "minio",
            "AWS_SECRET_ACCESS_KEY": "minio123",
            "AWS_REGION": "us-east-1",
            "AWS_ENDPOINT_URL": "http://10.5.0.5:9000",
            "AWS_ALLOW_HTTP": "TRUE",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "TRUE",
        }

    def read(self, bucket: str, table) -> pl.DataFrame:
        """
        Read a Delta table from S3.

        Args:
            bucket (str): The name of the S3 bucket.
            table: The name of the Delta table.

        Returns:
            pl.DataFrame: The DataFrame representing the Delta table.
        """

        uri = f"s3://{bucket}/{table}"
        
        try:
            df = pl.read_delta(
                uri,
                storage_options=self.storage_options
             )
            
        except Exception as e:
            print(e)
            df = pl.DataFrame([
                    pl.Series("blank", [], dtype=pl.Int32),
                ])
        
        return df

    def write(self, df: pl.DataFrame, bucket: str, table: str, mode: str, overwrite_schema: bool) -> None:
        """
        Write a Delta table to S3.

        Args:
            df (pl.DataFrame): The DataFrame to be written.
            bucket (str): The name of the S3 bucket.
            table (str): The name of the Delta table.
            mode (str): The write mode (e.g., 'overwrite', 'append', 'ignore').
            overwrite_schema (bool): Whether to overwrite the schema of the Delta table.

        Returns:
            None
        """

        uri = f"s3://{bucket}/{table}"
        
        try:
            df.write_delta(
                uri,
                mode=mode,
                overwrite_schema=overwrite_schema,
                storage_options=self.storage_options,
            )

        except Exception as e:
            print(e)

        return None