import polars as pl
from datetime import datetime
import plotly.express as px

def generate_image() -> None:
    """
    Generate a chart
    """

    df = pl.DataFrame(
        {
            "date": [
                datetime(2025, 1, 1),
                datetime(2025, 1, 2),
                datetime(2025, 1, 3),
            ],
            "float": [4.0, 5.0, 6.0],
        }
    )

    fig = px.line(df, y="float", title="Testing")

    fig.write("image.png")

    return None