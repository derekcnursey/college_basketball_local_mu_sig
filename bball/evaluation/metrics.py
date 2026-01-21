import numpy as np
import pandas as pd
from sklearn.metrics import mean_squared_error


def mse_vs_vegas(y_pred, y_vegas):
    return mean_squared_error(y_vegas, y_pred)


def roi_at_edge(df: pd.DataFrame, edge_col: str, threshold: float):
    """
    Simple ROI calculation for spread bets where |edge| >= threshold.

    df needs columns: ['edge', 'bet_result'] where bet_result is
    +1 (win) or -1 (loss).
    """
    subset = df.loc[df[edge_col].abs() >= threshold]
    if subset.empty:
        return 0.0
    return subset["bet_result"].mean()
