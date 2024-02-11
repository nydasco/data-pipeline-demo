#!/usr/bin/env python

import polars as pl
import params

def extract_from_delta(from_rate = str, to_rate = str) -> pl.DataFrame:
    """
    Get exchange rates from Delta
    """

    uri = f"s3://silver/{from_rate}to{to_rate}"

    df = pl.read_delta(
            uri,
            storage_options = params.storage_options
         )

    return df

def load_dim_currency(df) -> pl.SQLContext:
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
        storage_options = params.storage_options,
    )

    return data

def main():
    dfs = []

    for pair in params.rates:
        for key in pair:
            df = extract_from_delta(key, pair[key])
            dfs.append(df)
            print(key, "->", pair[key], "complete.")

    combined_df = pl.concat(dfs)

    dim_currency = load_dim_currency(combined_df)

    print(dim_currency)


if __name__ == "__main__":
    main()