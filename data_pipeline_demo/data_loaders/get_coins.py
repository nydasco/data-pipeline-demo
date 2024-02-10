import io
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Get a list of coins from the CoinGecko API
    """
    url = 'https://api.coingecko.com/api/v3/coins/list'
    response = requests.get(url)
    data = response.text
    return data
