"""
Command-line entry points for daily batch jobs:
    ingest → build features → train → predict → evaluate
"""
import click
from dotenv import load_dotenv
from bball.models.tuner import tune
load_dotenv()

from bball.data.loaders import (
    load_training_dataframe,
    train_val_split,
)
from bball.models.trainer import fit_regressor, fit_classifier
from bball.models.infer import load_models, predict_margin, predict_home_win_prob
import json, joblib
from pathlib import Path
from sklearn.preprocessing import StandardScaler
import pandas as pd
import json, joblib, pandas as pd, torch
from pathlib import Path
from bball.data.loaders import load_season_data            # your loader
from bball.models.infer   import load_models, predict_margin, predict_home_win_prob
from predict_games import build_today_feature_frame, attach_hard_rock_lines


TARGET_REG = "spread_home"   # adjust to your column names
TARGET_CLS = "home_win"
INFO_COLS = [
    "date",
    "away_team_name",
    "home_team_name",
    "away_team_pts",
    "home_team_pts",
]

@click.group()
def cli():
    """BBall daily pipeline CLI"""


@cli.command()
@click.option(
    "--trials",
    default=50,
    show_default=True,
    help="Optuna trials",
)
def tune_cmd(trials: int):
    """Hyperparameter tuning for torch regressor."""
    tune(trials=trials)


@cli.command()
def ingest():
    """Placeholder: your ingest job (if any)."""
    click.echo("ingest: not implemented here")


@cli.command()
@click.option(
    "--season",
    "season_year",
    default=2026,
    show_default=True,
    help="Torvik season key, e.g., 2026 for 2025–26 season",
)
@click.option(
    "--out",
    default="features_season.parquet",
    show_default=True,
    help="Where to write the season feature frame",
)
def build_features(season_year: int, out: str):
    """
    Build per-game feature frame for an entire season.

    Writes a parquet for training / backtesting.
    """
    import pandas as pd
    from pathlib import Path

    df = load_season_data(season_year=season_year)
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    print(f"✓ wrote {len(df):,} rows → {out}")


@cli.command("train")
@click.option(
    "--season",
    "season_year",
    default=2026,
    show_default=True,
    help="Torvik season key, e.g., 2026 for 2025–26 season",
)
@click.option(
    "--epochs",
    default=50,
    show_default=True,
    help="Torch epochs",
)
def train_cmd(season_year: int, epochs: int):
    """
    Train both regressor and classifier models for a season.

    Saves artifacts to ./artifacts by default (see bball.models.trainer).
    """
    df = load_training_dataframe(season_year=season_year)
    train_df, val_df = train_val_split(df)
    fit_regressor(train_df, val_df, epochs=epochs)
    fit_classifier(train_df, val_df, epochs=epochs)
    print("✓ training complete")


@cli.command("predict-season")
@click.option(
    "--season",
    "season_year",
    default=2026,
    show_default=True,
    help="Torvik season key, e.g., 2026 for 2025–26 season",
)
@click.option(
    "--out",
    default="preds_season.csv",
    show_default=True,
    help="Where to save season predictions",
)
def predict_season(season_year: int, out: str):
    """
    Predict all eligible games in a season data frame.
    """
    import json, joblib
    import numpy as np
    import pandas as pd
    from pathlib import Path

    # 1️⃣ Load season data (features + info)
    df = load_season_data(season_year=season_year)

    if df.empty:
        print("No season data found.")
        return

    # 2️⃣ Align features + scale using saved feature order (and scaler if present)
    feats_order = json.load(open("artifacts/feature_order.json"))
    info_df = df[INFO_COLS].copy()
    X_df = df.drop(columns=[c for c in INFO_COLS if c in df.columns]).copy()
    X_feats = X_df.reindex(columns=feats_order)

    try:
        scaler = joblib.load("artifacts/scaler.pkl")
        X_model = pd.DataFrame(
            scaler.transform(X_feats),
            columns=feats_order,
            index=X_feats.index,
        )
    except FileNotFoundError:
        X_model = X_feats

    # 3️⃣ Load models
    reg, cls = load_models()

    # 4️⃣ Predict margin + sigma + win prob
    mu, sigma = predict_margin(reg, X_model)
    p_home = predict_home_win_prob(cls, X_model)

    # 5️⃣ Build output frame
    df_out = info_df.copy()
    df_out["pred_margin"] = mu
    df_out["pred_sigma"] = sigma
    df_out["pred_home_win_prob"] = p_home

    # 6️⃣ Attach lines + edges (optional)
    df_out = attach_hard_rock_lines(df_out, pred_col="pred_margin")

    # 7️⃣  persist
    Path(out).parent.mkdir(exist_ok=True)
    df_out.to_csv(out, index=False)
    print(f"✓ wrote {len(df_out):,} rows → {out}")


@cli.command("predict-today")
@click.option(
    "--season",
    "season_year",
    default=2026,
    show_default=True,
    help="Torvik season key, e.g., 2026 for 2025–26 season",
)
@click.option(
    "--out",
    default=None,
    show_default=False,
    help="Where to save today's predictions (default: predictions/preds_YYYY_M_D_edge.csv)",
)
def predict_today(season_year: int, out: str | None):
    """
    Generate model predictions for *today's* games only.
    """
    import json, joblib
    import pandas as pd
    from pathlib import Path
    from datetime import datetime

    # 1️⃣ Build today's feature frame
    info_df, X_df = build_today_feature_frame(season_year=season_year)

    if X_df.empty:
        print("No eligible D1 games found for today in the super sked.")
        return

    # 2️⃣ Align features and scale (same as predict-season)
    feats_order = json.load(open("artifacts/feature_order.json"))
    X_feats = X_df.reindex(columns=feats_order)

    try:
        scaler = joblib.load("artifacts/scaler.pkl")
        X_model = pd.DataFrame(
            scaler.transform(X_feats),
            columns=feats_order,
            index=X_feats.index,
        )
    except FileNotFoundError:
        X_model = X_feats

    # 3️⃣ Load
    reg, cls = load_models()

    # 4️⃣ Predict
    mu, sigma = predict_margin(reg, X_model)
    p_home = predict_home_win_prob(cls, X_model)

    # 5️⃣ Build output frame
    df_out = info_df.copy()
    df_out["pred_margin"] = mu
    df_out["pred_sigma"] = sigma
    df_out["pred_home_win_prob"] = p_home

    # 6️⃣ Attach Hard Rock lines and compute edges
    df_out = attach_hard_rock_lines(df_out, pred_col="pred_margin")


    # ✅ Cover probabilities (only possible after lines are attached)
    # Home covers if (margin + home_spread_num) > 0
    # =========================
    # Spread edge + cover probs
    # =========================
    # edge_points = mu + home_spread_num
    #   > 0  => home covers more often than not
    #   < 0  => away covers more often than not

    import numpy as np
    from scipy.stats import norm

    if "home_spread_num" in df_out.columns and "pred_sigma" in df_out.columns and "pred_margin" in df_out.columns:
        mu_arr = df_out["pred_margin"].astype(float).to_numpy()
        sigma_safe = np.clip(df_out["pred_sigma"].astype(float).to_numpy(), 1e-6, None)
        spread_num = df_out["home_spread_num"].astype(float).to_numpy()

        # Edge in points (mu + spread)
        edge_points = mu_arr + spread_num
        df_out["edge_points"] = edge_points

        # P(home covers) = P(margin + spread > 0) = P(N(mu, sigma) > -spread)
        z = (mu_arr + spread_num) / sigma_safe
        p_home_covers = norm.cdf(z)
        df_out["home_cover_prob"] = p_home_covers
        df_out["away_cover_prob"] = 1.0 - p_home_covers

        # Fair odds (decimal) for covering
        # Avoid divide by zero
        eps = 1e-9
        df_out["home_cover_fair_odds"] = 1.0 / np.clip(p_home_covers, eps, 1.0)
        df_out["away_cover_fair_odds"] = 1.0 / np.clip(1.0 - p_home_covers, eps, 1.0)

        # Pick side based on higher cover probability (equivalently sign of edge)
        df_out["pick_side"] = np.where(edge_points >= 0, "home", "away")
        df_out["pick_cover_prob"] = np.where(edge_points >= 0, p_home_covers, 1.0 - p_home_covers)

        # If we have odds for each side, compute EV per $1
        # EV = p*(odds-1) - (1-p)
        if "home_spread_odds" in df_out.columns and "away_spread_odds" in df_out.columns:
            home_odds = df_out["home_spread_odds"].astype(float).to_numpy()
            away_odds = df_out["away_spread_odds"].astype(float).to_numpy()

            ev_home = p_home_covers * (home_odds - 1.0) - (1.0 - p_home_covers)
            ev_away = (1.0 - p_home_covers) * (away_odds - 1.0) - p_home_covers

            df_out["home_ev_per_1"] = ev_home
            df_out["away_ev_per_1"] = ev_away

            df_out["pick_ev_per_1"] = np.where(edge_points >= 0, ev_home, ev_away)
            df_out["pick_spread_odds"] = np.where(edge_points >= 0, home_odds, away_odds)

            # Pick "fair odds" for the chosen side
            df_out["pick_fair_odds"] = np.where(edge_points >= 0, df_out["home_cover_fair_odds"], df_out["away_cover_fair_odds"])

            # Probability edge (how much probability advantage vs implied probability)
            # implied prob from offered odds = 1/odds
            implied_home = 1.0 / np.clip(home_odds, eps, None)
            implied_away = 1.0 / np.clip(away_odds, eps, None)
            df_out["home_prob_edge"] = p_home_covers - implied_home
            df_out["away_prob_edge"] = (1.0 - p_home_covers) - implied_away
            df_out["pick_prob_edge"] = np.where(edge_points >= 0, df_out["home_prob_edge"], df_out["away_prob_edge"])

    # =========================
    # Column ordering / display
    # =========================
    edge_cols = [
        "spread_home",
        "spread_diff",
        "model_home_spread",
        "home_spread_num",
        "home_spread_odds",
    ]

    front_cols = [
        "date",
        "away_team_name",
        "home_team_name",
        "neutral_site",
    ] + [c for c in edge_cols if c in df_out.columns] + [
        "pred_margin",
        "pred_sigma",
        "pred_home_win_prob",
    ]

    rest_cols = [c for c in df_out.columns if c not in front_cols]
    df_out = df_out[front_cols + rest_cols]


    # Default output path: repo_root/predictions/preds_YYYY_M_D_edge.csv
    if out is None or str(out).strip() == "":
        today = datetime.now().date()
        out_path = Path("predictions") / f"preds_{today.year}_{today.month}_{today.day}_edge.csv"
    else:
        out_path = Path(out)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_path, index=False)
    print(f"✓ wrote {len(df_out):,} rows → {out_path}")



if __name__ == "__main__":
    cli()
