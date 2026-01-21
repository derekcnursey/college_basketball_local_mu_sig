"""
Data-loading utilities for the College Basketball project.

Key entry points
----------------
load_training_dataframe()     → full pandas DataFrame with engineered targets
split_X_y(df, target_reg, target_cls) → X, y_reg, y_cls split
train_val_split(df, target_reg, target_cls,
                test_size=0.2, random_state=42) → train / val sets
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Tuple
from decimal import Decimal

import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split


# --------------------------------------------------------------------------- #
# 1. Build SQLAlchemy engine from .env credentials
# --------------------------------------------------------------------------- #
def _sa_engine():
    user = os.getenv("BBALL_DB_USER", "root")
    pwd  = os.getenv("BBALL_DB_PASS", "")
    host = os.getenv("BBALL_DB_HOST", "localhost")
    db   = os.getenv("BBALL_DB_NAME", "sports")
    return create_engine(f"mysql+pymysql://{user}:{pwd}@{host}/{db}")


# --------------------------------------------------------------------------- #
# 2. Main loader
# --------------------------------------------------------------------------- #
def load_training_dataframe() -> pd.DataFrame:
    """
    Loads `sports.training_data` from MySQL, or `training_data.csv`
    if the DB is unreachable.  Returns a fully numeric DataFrame with
    engineered targets.
    """
    csv_fallback = Path("training_data.csv")

    try:
        sql = "SELECT * FROM sports.training_data"
        df = pd.read_sql(sql, _sa_engine(), coerce_float=True)
        df["away_team_pts"] = pd.to_numeric(df["away_team_pts"], errors="coerce").fillna(0).astype("int64")
        df["home_team_pts"] = pd.to_numeric(df["home_team_pts"], errors="coerce").fillna(0).astype("int64")
        df["MOV"] = df["away_team_pts"] - df["home_team_pts"]
        df["spread_home"] = -df["MOV"] 
        df["total_pts"] = df["away_team_pts"] + df["home_team_pts"]
        df["home_win"] = (df["home_team_pts"] > df["away_team_pts"]).astype("int8") 
        df["home_team_home"] = df["neutral_site"].eq(0)
        df["away_team_home"] = False
        df = df.drop(columns=['date', 'MOV', 'total_pts' , 'away_team_name', 'home_team_name', 'away_team_pts', 'home_team_pts'])
        print(f"✓ Loaded {len(df):,} rows from MySQL")
    except Exception as err:
        if csv_fallback.exists():
            print(f"⚠️  MySQL failed ({err!s}); using {csv_fallback}")
            df = pd.read_csv(csv_fallback)
        else:
            raise RuntimeError(
                "Could not load training data from MySQL and fallback CSV "
                "does not exist."
            ) from err

    # ------------------------------------------------------------------- #
    # Feature engineering identical to legacy script
    # ------------------------------------------------------------------- #
    
    
    df = df.apply(pd.to_numeric, errors="raise")

    # Make sure booleans are int8
    bool_cols = df.select_dtypes("bool").columns
    df[bool_cols] = df[bool_cols].astype("int8")
    return df


# --------------------------------------------------------------------------- #
# 3. Convenience split helpers
# --------------------------------------------------------------------------- #
def split_X_y(
    df: pd.DataFrame, target_reg: str, target_cls: str
) -> Tuple[pd.DataFrame, pd.Series, pd.Series]:
    """
    Splits DataFrame into feature matrix X and two target Series.

    Parameters
    ----------
    df : pd.DataFrame
    target_reg : str
        Column name for regression target (e.g., 'MOV').
    target_cls : str
        Column name for classification target (e.g., 'home_win').

    Returns
    -------
    X, y_reg, y_cls
    """
    X = df.drop(columns=[target_reg, target_cls])
    y_reg = df[target_reg]
    y_cls = df[target_cls]
    return X, y_reg, y_cls


def train_val_split(
    df: pd.DataFrame,
    target_reg: str,
    target_cls: str,
    test_size: float = 0.2,
    random_state: int = 42,
):
    """
    Convenience wrapper: returns train / val splits for X, y_reg, y_cls.
    """
    X, y_reg, y_cls = split_X_y(df, target_reg, target_cls) 
    return train_test_split(
        X,
        y_reg,
        y_cls,
        test_size=test_size,
        random_state=random_state,
        stratify=y_cls,  # keep class balance
    )

def load_season_data(season: int):
    cutoff = str(season - 1) + "-10-01"
    sql = f"SELECT * FROM sports.training_data WHERE date > '{cutoff}';"
    df = pd.read_sql(sql, _sa_engine(), coerce_float=True)
    df["away_team_pts"] = pd.to_numeric(df["away_team_pts"], errors="coerce").fillna(0).astype("int64")
    df["home_team_pts"] = pd.to_numeric(df["home_team_pts"], errors="coerce").fillna(0).astype("int64")
    df["MOV"] = df["away_team_pts"] - df["home_team_pts"]
    df["spread_home"] = -df["MOV"] 
    df["total_pts"] = df["away_team_pts"] + df["home_team_pts"]
    df["home_win"] = (df["home_team_pts"] > df["away_team_pts"]).astype("int8") 
    df["home_team_home"] = df["neutral_site"].eq(0)
    df["away_team_home"] = False
    df = df.drop(columns=['MOV', 'total_pts'])
    return df