#!/usr/bin/env python

import polars as pl
from dags.params import Params
import hashlib

def hash_currency(currency_value) -> str:
    encoded_value = str(currency_value).encode()
    out = hashlib.sha256(encoded_value).hexdigest()

    return out

def extract_from_delta(from_rate = str, to_rate = str) -> pl.DataFrame:
    """
    Get exchange rates from Delta
    """
    
    uri = f"s3://bronze/{from_rate}to{to_rate}"

    df = pl.read_delta(
            uri, 
            storage_options = Params.storage_options
         )

    return df

def transform(df: pl.DataFrame, from_rate: str, to_rate: str) -> pl.DataFrame:
    """
    Transform the data
    """

    try:
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
        
        # Hash the currency values
        df = df.with_columns(pl.col("from_currency").apply(hash_currency).alias("from_currency_hash"))
        df1 = df.with_columns(pl.col("to_currency").apply(hash_currency).alias("to_currency_hash"))
        
        df2 = df1.select(
            "date",
            "from_currency",
            "from_currency_hash",
            "to_currency",
            "to_currency_hash",
            pl.col("open").cast(pl.Float32),
            pl.col("high").cast(pl.Float32),
            pl.col("low").cast(pl.Float32),
            pl.col("close").cast(pl.Float32)
        ).sort("date", descending = False)
    
    except Exception as e:
        print(e)
        df = pl.DataFrame([
                pl.Series("blank", [], dtype=pl.Int32),
            ])
    
    return df2

def load_to_delta(df: pl.DataFrame, from_rate: str, to_rate: str) -> None:
    """
    Load the data into Delta
    """
    
    uri = f"s3://silver/{from_rate}to{to_rate}"

    df.write_delta(
        uri,
        mode = "overwrite",
        overwrite_schema = True,
        storage_options = Params.storage_options,
    )

def main():
    for pair in Params.rates:
        for key in pair:
            df = extract_from_delta(key, pair[key])
            if df.height != 0:
                df1 = transform(df, key, pair[key])
                load_to_delta(df1, key, pair[key])
                print(key, "->", pair[key], "complete.")
            else:
                print(key, "->", pair[key], "failed.")

if __name__ == "__main__":
    main()