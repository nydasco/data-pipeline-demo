#!/usr/bin/env python

import polars as pl
import requests
import io
import params

def extract_from_api() -> pl.DataFrame:
    """
    Get a historical list of exchange rates from an API
    """

    url = f"https://www.alphavantage.co/query?function=FX_DAILY&from_symbol=AUD&to_symbol=USD&apikey={params.api_key}"
    response = requests.get(url)
    data = response.text

    df = pl.read_json(
            io.StringIO(data), 
            infer_schema_length=None
         )
        
    return df

def load_to_delta(df) -> None:
    """
    Load the data into Delta
    """
    
    uri = "s3://bronze/audtousd"

    df.write_delta(
        uri,
        mode = "append",
        overwrite_schema = True,
        storage_options = params.storage_options,
    )

def main():
    df = extract_from_api()
    load_to_delta(df)

if __name__ == "__main__":
    main()