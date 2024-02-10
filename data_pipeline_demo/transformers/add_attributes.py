import polars as pl
import io
from datetime import datetime

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Read the json data into a Polars DataFrame, and add the current datetime as a new field
    """
    df = pl.read_json(io.StringIO(data))
    get_date = datetime.now()

    df1 = df.with_columns(load_date = get_date)

    return df1


@test
def test_output(output, *args) -> None:
    """
    Test that the data exists
    """
    assert output is not None, 'The output is undefined'
