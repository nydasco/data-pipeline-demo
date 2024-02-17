#!/usr/bin/env python

import polars as pl
from pipelines.delta import DeltaS3

def load_dim_currency(df) -> None:
    """
    Saves a Kimball dimension for currencies

    Args:
        df (DataFrame): The input DataFrame containing currency data.

    Returns:
        list: A list of dictionaries representing the currency dimension data.
    """

    sql = pl.SQLContext(df = df).execute(
        """
        SELECT DISTINCT
            code_hash AS dim_currency_id,
            code AS currency_code,
            currency AS currency_name
        FROM
            df
        """
    )

    data = sql.collect()

    return data

def load_fct_rates(df) -> None:
    """
    Saves a Kimball fact for rates

    Parameters:
        df (DataFrame): The input DataFrame containing the rates data.

    Returns:
        data (List): A list of collected data from the SQL query.
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
    This function reads rates and currency data from a Silver bucket, processes it, and writes the results to a Gold bucket.
    It loads the processed rates into the 'fct_rates' table and the currency data into the 'dim_currency' table.
    """
    deltas3 = DeltaS3()
    df_rates = deltas3.read(bucket="silver", table="rates")
    df_currency = deltas3.read(bucket="silver", table="currency_list")
    try:
        fct_rates = load_fct_rates(df_rates)
        deltas3.write(fct_rates,
                      bucket="gold",
                      table="fct_rates",
                      mode="overwrite",
                      overwrite_schema=True)
        print("fct_rates loaded")

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