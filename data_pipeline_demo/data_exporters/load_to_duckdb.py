from mage_ai.io.file import FileIO
import duckdb

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_duckdb(data, **kwargs) -> None:
    """
    Template for exporting data to filesystem.

    Docs: https://docs.mage.ai/design/data-loading#fileio
    """
    
    con = duckdb.connect("duckdb.db")
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
    
