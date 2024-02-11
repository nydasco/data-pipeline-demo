#!/usr/bin/env python

import polars as pl
import params

def extract_from_delta(from_rate = str, to_rate = str) -> pl.DataFrame:
    """
    Get exchange rates from Delta
    """
    
    uri = f"s3://bronze/{from_rate}to{to_rate}"

    df = pl.read_delta(
            uri, 
            storage_options = params.storage_options
         )

    return df

def transform(df: pl.DataFrame, from_rate: str, to_rate: str) -> pl.DataFrame:
    """
    Transform the data
    """

    df = df.unnest("Time Series FX (Daily)") \
            .melt(
                id_vars = "Meta Data", 
                variable_name = "Date",
                ) \
            .unnest("value") \
            .select(pl.all().exclude("Meta Data")
            )
    
    df = df.with_columns(
                from_currency = pl.lit(from_rate),
                to_currency = pl.lit(to_rate),
                ) \
            .rename(
                {
                    "Date": "date", 
                    "1. open": "open", 
                    "2. high": "high", 
                    "3. low": "low", 
                    "4. close": "close"
                }
                )
    
    df = df.select(
        "date",
        "from_currency",
        "to_currency",
        "open",
        "high",
        "low",
        "close"
    ).sort("date", descending = False)
    
    return df

def load_to_delta(df: pl.DataFrame, from_rate: str, to_rate: str) -> None:
    """
    Load the data into Delta
    """
    
    uri = f"s3://silver/{from_rate}to{to_rate}"

    df.write_delta(
        uri,
        mode = "overwrite",
        overwrite_schema = True,
        storage_options = params.storage_options,
    )

def main():
    for pair in params.rates:
        for key in pair:
            df = extract_from_delta(key, pair[key])
            df1 = transform(df, key, pair[key])
            load_to_delta(df1, key, pair[key])
            print(key, "->", pair[key], "complete.")

if __name__ == "__main__":
    main()