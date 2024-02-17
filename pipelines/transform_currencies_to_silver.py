#!/usr/bin/env python

import polars as pl
import hashlib
from pipelines.delta import DeltaS3

import hashlib

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

def transform(df: pl.DataFrame) -> pl.DataFrame:
    """
    Transform the data by adding a hash value of the currency code.
    
    Args:
        df (pl.DataFrame): The input DataFrame containing the data to be transformed.
        
    Returns:
        pl.DataFrame: The transformed DataFrame.
    """

    try:
        # Rename the columns to lowercase
        df1 = df.rename(
                        {
                            "Code": "code", 
                            "Currency": "currency"
                        }
                )

        # Hash the currency code
        df2 = df1.with_columns(pl.col("code").apply(hash_column).alias("code_hash"))
        
        # Select the columns
        df3= df2.select(
            "code",
            "code_hash",
            "currency"
        ).sort("code", descending = False)

        print(df3.head(5))
    
    except Exception as e:
        print(e)
        df3 = pl.DataFrame([
                pl.Series("blank", [], dtype=pl.Int32),
            ])
    
    return df3

def main():
    """
    Main function that transforms the currency_list DataFrame and loads it to Delta table.
    """
    deltas3 = DeltaS3()

    df = deltas3.read(bucket = "bronze", table = "currency_list")

    print("Transforming currency_list")
    df1 = transform(df)

    print("Loading to Delta")
    deltas3.write(df1, 
                  bucket = "silver", 
                  table = "currency_list", 
                  mode = "overwrite", 
                  overwrite_schema = True)

if __name__ == "__main__":
    main()