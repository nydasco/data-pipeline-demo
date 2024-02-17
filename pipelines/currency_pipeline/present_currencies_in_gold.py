#!/usr/bin/env python

import polars as pl

from pipelines.delta import DeltaS3

def load_dim_currency(df) -> pl.DataFrame:
    """
    Saves a Kimball dimension for currencies

    Args:
        df (DataFrame): The input DataFrame containing currency data.

    Returns:
        data (DataFrame): A list of dictionaries representing the currency dimension data.
    """

    sql = pl.SQLContext(df = df).execute(
        """
        SELECT DISTINCT
            code_hash AS dim_currency_id,
            code AS currency_code,
            currency AS currency_name
        FROM
            df
        -- Add unknown and not available currencies (for error handling)
        UNION
        SELECT
            '-1' AS dim_currency_id,
            'UNKNOWN' AS currency_code,
            'UNKNOWN' AS currency_name
        UNION
        SELECT
            '-2' AS dim_currency_id,
            'NOT_AVAILABLE' AS currency_code,
            'NOT_AVAILABLE' AS currency_name
        """
    )

    data = sql.collect()

    return data

def main():
    """
    This function reads currency data from a Silver bucket, processes it, and writes the results to a Gold bucket.
    It loads the processed currency data into the 'dim_currency' table.
    """
    deltas3 = DeltaS3()
    df_currency = deltas3.read(bucket="silver", table="currency_list")
    try:
        dim_currency = load_dim_currency(df_currency)
        deltas3.write(dim_currency,
                      bucket="gold",
                      table="dim_currency",
                      mode="overwrite",
                      overwrite_schema=True)
        print("dim_currency loaded")

    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()