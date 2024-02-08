import io
import duckdb
import pandas as pd
import json
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Get a list of coins from the API
    """
    url = 'https://api.coingecko.com/api/v3/coins/bitcoin/history?date=07-02-2024'
    response = requests.get(url)
    #df = pd.read_json(io.StringIO(response.text))
    data = io.StringIO(response.text)
    df = pd.read_json(data)

    return df


@test
def test_output(output, *args) -> None:
    """
    Ensure data is loaded
    """
    assert output is not None, 'The output is undefined'
