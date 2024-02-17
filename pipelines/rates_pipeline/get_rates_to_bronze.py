#!/usr/bin/env python

import polars as pl
import requests
import io

from pipelines.params import Params
from pipelines.delta import DeltaS3

def extract_from_api(from_rate = str, to_rate = str) -> pl.DataFrame:
    """
    Get a historical list of exchange rates from an API

    Parameters:
    from_rate (str): The base currency symbol.
    to_rate (str): The target currency symbol.

    Returns:
    pl.DataFrame: A DataFrame containing the historical exchange rates.
    """

    url = f"https://www.alphavantage.co/query?function=FX_DAILY&from_symbol={from_rate}&to_symbol={to_rate}&outputsize=full&apikey={Params.api_key}"
    try:
        response = requests.get(url)
        data = response.text

        df = pl.read_json(
                io.StringIO(data), 
                infer_schema_length=None
             )
    except Exception as e:
        print(e)
        df = pl.DataFrame([
                pl.Series("blank", [], dtype=pl.Int32),
            ])
    
    print(df.head(5))
    
    return df

def main():
    """
    This function is the entry point of the data pipeline.
    It extracts data from an API for each rate pair specified in Params.rates,
    writes the extracted data to a Delta table in the "bronze" bucket,
    and prints the status of each extraction process.
    """
    deltas3 = DeltaS3()
    for pair in Params.rates:
        for key in pair:
            df = extract_from_api(key, pair[key])
            if df.height != 0:
                deltas3.write(df, 
                              bucket = "bronze",
                              table = f"{key}to{pair[key]}",
                              mode = "overwrite",
                              overwrite_schema = True)
                
                print(key, "->", pair[key], "complete.")
            else:
                print(key, "->", pair[key], "failed. Check the API key and the rate pair.\n", str(df))

if __name__ == "__main__":
    main()