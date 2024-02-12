#!/usr/bin/env python

import polars as pl
import requests
import io
import params

def extract_from_api(from_rate = str, to_rate = str) -> pl.DataFrame:
    """
    Get a historical list of exchange rates from an API
    """

    url = f"https://www.alphavantage.co/query?function=FX_DAILY&from_symbol={from_rate}&to_symbol={to_rate}&apikey={params.api_key}"
    response = requests.get(url)
    data = response.text

    df = pl.read_json(
            io.StringIO(data), 
            infer_schema_length=None
         )
    
    return df

def load_to_delta(df: pl.DataFrame, from_rate: str, to_rate: str) -> None:
    """
    Load the data into Delta
    """
    
    uri = f"s3://bronze/{from_rate}to{to_rate}"

    df.write_delta(
        uri,
        mode = "append",
        overwrite_schema = True,
        storage_options = params.storage_options,
    )

def main():
    for pair in params.rates:
        for key in pair:
            df = extract_from_api(key, pair[key])
            load_to_delta(df, key, pair[key])
            print(key, "->", pair[key], "complete.")

if __name__ == "__main__":
    main()