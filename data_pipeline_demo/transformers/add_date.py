import duckdb

from datetime import datetime
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    con = duckdb.connect("duckdb.db")
    #print(con.sql("SELECT * FROM data"))
    duck_data = duckdb.sql("SELECT *, current_date AS load_date FROM data")

    con.sql("CREATE TABLE IF NOT EXISTS coins (id VARCHAR, symbol VARCHAR, name VARCHAR, load_date DATE)")

    con.sql(
        """
        INSERT INTO coins
        SELECT
            *
        FROM
            duck_data 
        """)
    

    return con.sql("SELECT * FROM coins").to_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
