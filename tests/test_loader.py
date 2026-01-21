import pandas as pd
from bball.data.loaders import load_training_dataframe

def test_loader_shapes():
    df = load_training_dataframe()
    # sanity checks
    assert len(df) > 1000, "not enough rows"
    assert "spread_home" in df.columns
    # no object dtypes
    assert not any(df.dtypes == "object")
