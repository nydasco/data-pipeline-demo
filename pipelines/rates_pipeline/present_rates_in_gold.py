#!/usr/bin/env python

import polars as pl
from datetime import date

from pipelines.delta import DeltaS3

def load_fct_rates(df) -> pl.DataFrame:
    """
    Saves a Kimball fact for rates

    Parameters:
        df (DataFrame): The input DataFrame containing the rates data.

    Returns:
        data (DataFrame): A list of collected data from the SQL query.
    """
    sql = pl.SQLContext(df = df).execute(
        """
        SELECT 
            date,
            from_currency_hash AS dim_currency_id_from,
            to_currency_hash AS dim_currency_id_to,
            open AS opening_rate,
            close - open AS movement
        FROM
            df
        """
    )

    data = sql.collect()

    return data

def main():
    """
    This function reads rates data from a Silver bucket, processes it, and writes the results to a Gold bucket.
    It loads the processed rates into the 'fct_rates' table.
    """
    deltas3 = DeltaS3()
    df_rates = deltas3.read(bucket="silver", table="rates")
    try:
        fct_rates = load_fct_rates(df_rates)
        deltas3.write(fct_rates,
                      bucket="gold",
                      table="fct_rates",
                      mode="overwrite",
                      overwrite_schema=True)
        print("fct_rates loaded")

    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()