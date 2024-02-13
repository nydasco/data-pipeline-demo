#!/usr/bin/env python

import polars as pl
from dags.params import Params

def extract_from_delta(from_rate = str, to_rate = str) -> pl.DataFrame:
    """
    Get exchange rates from Delta
    """

    uri = f"s3://silver/{from_rate}to{to_rate}"

    df = pl.read_delta(
            uri,
            storage_options = Params.storage_options
         )

    return df

def load_dim_currency(df) -> None:
    """
    Saves a Kimball dimension for currencies
    """

    sql = pl.SQLContext(df = df).execute(
        """
        SELECT DISTINCT
            from_currency AS currency_code
        FROM
            df
        UNION DISTINCT
        SELECT DISTINCT
            to_currency AS currency_code
        FROM
            df
        """
    )

    data = sql.collect()
    
    uri = f"s3://gold/dim_currency"

    data.write_delta(
        uri,
        mode = "append",
        overwrite_schema = True,
        storage_options = Params.storage_options,
    )

    return None

def load_fct_rates(df) -> None:
    """
    Saves a Kimball fact for rates
    """

    sql = pl.SQLContext(df = df).execute(
        """
        SELECT 
            date,
            from_currency AS dim_currency_from,
            to_currency AS dim_currency_to,
            open AS opening_rate,
            close AS closing_rate
        FROM
            df
        """
    )

    data = sql.collect()
    
    uri = f"s3://gold/fct_rates"

    data.write_delta(
        uri,
        mode = "append",
        overwrite_schema = True,
        storage_options = Params.storage_options,
    )

    return None

def main():
    dfs = []

    for pair in Params.rates:
        for key in pair:
            df = extract_from_delta(key, pair[key])
            dfs.append(df)
            print(key, "->", pair[key], "complete.")

    combined_df = pl.concat(dfs)

    load_dim_currency(combined_df)
    load_fct_rates(combined_df)



if __name__ == "__main__":
    main()