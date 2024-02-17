#!/usr/bin/env python

import polars as pl

from pipelines.delta import DeltaS3

def extract_from_csv(file = str) -> pl.DataFrame:
    """
    Get a list of currencies from a CSV in the seed folder

    Parameters:
        file (str): The name of the CSV file to extract data from

    Returns:
        pl.DataFrame: A DataFrame containing the extracted data from the CSV file
    """

    path = f"/opt/airflow/seeds/{file}.csv"
    try:
        df = pl.read_csv(path)
    except Exception as e:
        print(e)
        df = pl.DataFrame([
                pl.Series("blank", [], dtype=pl.Int32),
            ])
        
    print(df.head(5))
    
    return df

def main():
    """
    This function extracts currency data from a CSV file and writes it to a Delta table in S3.
    If the extracted data is not empty, it overwrites the existing data in the table.
    """
    deltas3 = DeltaS3()
    df = extract_from_csv("currency_list")
    if df.height != 0:
        deltas3.write(df, 
                      bucket = "bronze", 
                      table = "currency_list",
                      mode = "overwrite",
                      overwrite_schema = True)
        
        print("Load complete.")
    else:
        print("Load failed")

if __name__ == "__main__":
    main()