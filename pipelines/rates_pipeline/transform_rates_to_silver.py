#!/usr/bin/env python

import polars as pl
import hashlib

from pipelines.params import Params
from pipelines.delta import DeltaS3

def hash_column(column_name) -> str:
    """
    Hashes the given column name using SHA256 algorithm.

    Args:
        column_name (str): The name of the column to be hashed.

    Returns:
        str: The hashed value of the column name.
    """
    encoded_value = str(column_name).encode()
    out = hashlib.sha256(encoded_value).hexdigest()

    return out

def transform(df: pl.DataFrame, from_rate: str, to_rate: str) -> pl.DataFrame:
    """
    Transform the data by performing the following steps:
    1. Unnest the Time Series FX (Daily) column.
    2. Unpivot the data and remove the Meta Data column.
    3. Rename the columns and add the currency values.
    4. Hash the currency values.
    5. Cast the data types.
    6. Sort the data by date in ascending order.

    Args:
        df (pl.DataFrame): The input DataFrame containing the data to be transformed.
        from_rate (str): The currency code of the source currency.
        to_rate (str): The currency code of the target currency.

    Returns:
        pl.DataFrame: The transformed DataFrame.
    """

    try:
        # Unnest the Time Series FX (Daily) column, then unpivot the data and remove the Meta Data column
        df = df.unnest("Time Series FX (Daily)") \
                .melt(
                    id_vars = "Meta Data", 
                    variable_name = "Date",
                    ) \
                .unnest("value") \
                .select(pl.all().exclude("Meta Data")
                )
        
        # Rename the columns and add the currency values
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
        df = df.with_columns(pl.col("from_currency").apply(hash_column).alias("from_currency_hash"))
        df1 = df.with_columns(pl.col("to_currency").apply(hash_column).alias("to_currency_hash"))
        
        # Cast the data types
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
        df2 = pl.DataFrame([
                pl.Series("blank", [], dtype=pl.Int32),
            ])
        
    print(df2.head(5))
    
    return df2

def main():
    """
    Extracts data from the bronze bucket, transforms it, and loads it into the silver bucket.

    This function iterates over the rates specified in the Params module, extracts the data from the bronze bucket,
    transforms it using the `transform` function, and appends the transformed dataframes to a list. The list of dataframes
    is then concatenated into a single dataframe. Finally, the combined dataframe is loaded into the silver bucket.

    Returns:
        None
    """

    deltas3 = DeltaS3()
    dfs = []
    for pair in Params.rates:
        for key in pair:

            print("Extracting", key, "->", pair[key])
            df = deltas3.read(bucket = "bronze", table = f"{key}to{pair[key]}")

            if df.height != 0:

                print("Transforming", key, "->", pair[key])
                df1 = transform(df, key, pair[key])

                print("Appending", key, "->", pair[key])
                dfs.append(df1)

                print(key, "->", pair[key], "complete.")
            else:
                print(key, "->", pair[key], "failed.")

    print("Combining dataframes")
    try:
        combined_df = pl.concat(dfs)
    except:
        print("Was unable to concatenate the DataFrames")
        return
    
    print("Loading to Delta")
    deltas3.write(combined_df, 
                  bucket = "silver", 
                  table = "rates", 
                  mode = "overwrite", 
                  overwrite_schema = True)

if __name__ == "__main__":
    main()