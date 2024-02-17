#!/usr/bin/env python

import polars as pl
from datetime import date

from pipelines.delta import DeltaS3

def load_dim_date() -> pl.DataFrame:
    """
    Saves a Kimball dimension for dates

    Returns:
        data (DataFrame): A DataFrame containing date and date-related data.
    """

    date_range =  pl.date_range(date(2010, 1, 1), date.today(), "1d", eager=True).alias("date").to_frame()

    data = date_range.with_columns([
        pl.col("date"),
        pl.col("date").dt.year().alias("year"),
        pl.col("date").dt.month().alias("month_number"),
        pl.col("date").dt.strftime("%B").alias("month_name"),
        pl.col("date").dt.day().alias("day_of_month"),
        pl.col("date").dt.strftime("%j").alias("day_of_year"),
        pl.col("date").dt.weekday().alias("weekday_number"),
        pl.col("date").dt.strftime("%A").alias("weekday_name"),
        pl.col("date").dt.strftime("%W").alias("week_number"),
    ]
    )

    return data

def main():
    """
    This function reads generates a date dimension table, and writes the results to a Gold bucket.
    It loads the processed rates into the 'dim_date' table.
    """
    deltas3 = DeltaS3()
    try:
        dim_date = load_dim_date()
        deltas3.write(dim_date,
                      bucket="gold",
                      table="dim_date",
                      mode="overwrite",
                      overwrite_schema=True)
        print("dim_date loaded")

    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()