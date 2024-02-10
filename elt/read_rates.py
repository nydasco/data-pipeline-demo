#!/usr/bin/env python

import polars as pl
import params

def extract_from_delta() -> pl.DataFrame:
    """
    Get exchange rates from Delta
    """
    
    uri = "s3://bronze/audtousd"

    df = pl.read_delta(uri, storage_options = params.storage_options)

    return df

def transform(df: pl.DataFrame) -> pl.DataFrame:
    """
    Transform the data
    """

    df = df.unnest("Time Series FX (Daily)").melt(id_vars = "Meta Data", variable_name = "Date").unnest("value").select(pl.all().exclude("Meta Data"))

    return df

def load_to_delta(df) -> None:
    """
    Load the data into Delta
    """
    
    uri = "s3://silver/audtousd"

    df.write_delta(
        uri,
        mode = "append",
        overwrite_schema = True,
        storage_options = params.storage_options,
    )

def main():
    df = extract_from_delta()
    df1 = transform(df)
    load_to_delta(df1)

if __name__ == "__main__":
    main()