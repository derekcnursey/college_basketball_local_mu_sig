"""
Command-line entry points for daily batch jobs:
    ingest → build features → train → predict → evaluate
"""
import math
import click
import numpy as np
import pandas as pd
import datetime as _dt
import subprocess
import sys
from pathlib import Path
from dotenv import load_dotenv

from bball.data.loaders import load_season_data, load_training_dataframe, train_val_split
from bball.models.infer import load_regressor, predict_margin_dist
from bball.models.trainer import fit_classifier, fit_regressor
from bball.models.tuner import tune
import predict_games as predict_games_mod
from predict_games import attach_hard_rock_lines, build_today_feature_frame

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parents[1]


TARGET_REG = "spread_home"   # adjust to your column names
TARGET_CLS = "home_win"
INFO_COLS = [
    "date",
    "away_team_name",
    "home_team_name",
    "away_team_pts",
    "home_team_pts",
]


def win_prob_from_mu_sigma(mu: np.ndarray, sigma: np.ndarray) -> np.ndarray:
    """
    Compute P(margin > 0) assuming Normal(mu, sigma).
    Uses math.erf for compatibility (numpy erf not always available).
    """
    mu = np.asarray(mu, dtype=float)
    sigma = np.asarray(sigma, dtype=float)
    sigma = np.clip(sigma, 1e-6, None)

    z = mu / sigma
    erf_vec = np.vectorize(math.erf)
    return 0.5 * (1.0 + erf_vec(z / math.sqrt(2.0)))


def normal_cdf(z: np.ndarray) -> np.ndarray:
    """
    Standard normal CDF using math.erf (numpy erf may be missing).
    """
    z = np.asarray(z, dtype=float)
    erf_vec = np.vectorize(math.erf)
    return 0.5 * (1.0 + erf_vec(z / math.sqrt(2.0)))


def american_to_breakeven_prob(odds: np.ndarray) -> np.ndarray:
    """
    Convert American odds to break-even probability (ignores vig structure; just bet-level break-even).
    -110 -> 0.5238
    +150 -> 0.4000
    """
    o = pd.to_numeric(odds, errors="coerce").to_numpy(dtype=float)
    out = np.full_like(o, np.nan, dtype=float)

    neg = o < 0
    pos = o > 0

    out[neg] = (-o[neg]) / ((-o[neg]) + 100.0)
    out[pos] = 100.0 / (o[pos] + 100.0)

    return out


def american_profit_per_1_staked(odds: np.ndarray) -> np.ndarray:
    """
    Profit (not return) per $1 staked if the bet wins.
    -110 -> 0.9091 profit
    +150 -> 1.5 profit
    """
    o = pd.to_numeric(odds, errors="coerce").to_numpy(dtype=float)
    out = np.full_like(o, np.nan, dtype=float)

    neg = o < 0
    pos = o > 0

    out[neg] = 100.0 / (-o[neg])
    out[pos] = o[pos] / 100.0

    return out


def prob_to_american(p: np.ndarray) -> np.ndarray:
    """
    Convert probability to "fair" American odds (no vig).
    Returns float odds; you can round later.
    """
    p = np.asarray(p, dtype=float)
    out = np.full_like(p, np.nan, dtype=float)

    p = np.clip(p, 1e-9, 1 - 1e-9)

    fav = p >= 0.5
    dog = ~fav

    out[fav] = -100.0 * (p[fav] / (1.0 - p[fav]))
    out[dog] = 100.0 * ((1.0 - p[dog]) / p[dog])

    return out


def run_repo_script(script_name: str) -> None:
    script_path = REPO_ROOT / script_name
    subprocess.run([sys.executable, str(script_path)], check=True, cwd=REPO_ROOT)


def _coerce_date(value: object) -> _dt.date:
    if isinstance(value, _dt.datetime):
        return value.date()
    if isinstance(value, _dt.date):
        return value
    if isinstance(value, str):
        return _dt.datetime.strptime(value, "%Y-%m-%d").date()
    raise ValueError("date must be a date/datetime or YYYY-MM-DD string")


def build_feature_frame_for_date(
    season_year: int,
    target_date: object,
):
    target = _coerce_date(target_date)
    old_year = predict_games_mod.CURR_YEAR
    old_month = predict_games_mod.CURR_MONTH
    old_day = predict_games_mod.CURR_DAY
    old_yesterday = predict_games_mod.yesterday

    try:
        predict_games_mod.CURR_YEAR = target.year
        predict_games_mod.CURR_MONTH = target.month
        predict_games_mod.CURR_DAY = target.day
        predict_games_mod.yesterday = _dt.datetime(target.year, target.month, target.day) - _dt.timedelta(days=1)
        return build_today_feature_frame(season_year=season_year)
    finally:
        predict_games_mod.CURR_YEAR = old_year
        predict_games_mod.CURR_MONTH = old_month
        predict_games_mod.CURR_DAY = old_day
        predict_games_mod.yesterday = old_yesterday


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

    # 3️⃣ Load regressor only
    reg, _feat_order = load_regressor(default_input_dim=len(feats_order))

    # 4️⃣ Predict margin + sigma + win prob (derived from regressor)
    mu, sigma = predict_margin_dist(X_model, reg)
    p_home = win_prob_from_mu_sigma(mu, sigma)

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


def predict_today_impl(
    season_year: int,
    out: str | None,
    target_date: object | None = None,
):
    """
    Generate model predictions for *today's* games only.
    """
    import json, joblib
    from pathlib import Path
    from datetime import datetime

    # 1️⃣ Build feature frame for target date
    if target_date is None:
        target_date = _dt.date.today()
    info_df, X_df = build_feature_frame_for_date(season_year=season_year, target_date=target_date)

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

    # 3️⃣ Load regressor only
    reg, _feat_order = load_regressor(default_input_dim=len(feats_order))

    # 4️⃣ Predict
    mu, sigma = predict_margin_dist(X_model, reg)
    p_home = win_prob_from_mu_sigma(mu, sigma)

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

    if "home_spread_num" in df_out.columns and "pred_sigma" in df_out.columns and "pred_margin" in df_out.columns:
        mu_arr = df_out["pred_margin"].astype(float).to_numpy()
        sigma_safe = np.clip(df_out["pred_sigma"].astype(float).to_numpy(), 1e-6, None)

        home_spread = pd.to_numeric(df_out["home_spread_num"], errors="coerce").to_numpy()
        away_spread = pd.to_numeric(df_out["away_spread_num"], errors="coerce").to_numpy()

        edge_points = mu_arr + home_spread
        df_out["edge_points"] = edge_points

        edge_z_home = edge_points / sigma_safe
        df_out["edge_z_home"] = edge_z_home
        df_out["edge_strength"] = np.abs(edge_z_home)

        df_out["home_cover_prob"] = normal_cdf(edge_z_home)
        df_out["away_cover_prob"] = 1.0 - df_out["home_cover_prob"]

        df_out["pick_side"] = np.where(df_out["edge_points"] >= 0, "HOME", "AWAY")
        df_out["pick_cover_prob"] = np.where(
            df_out["pick_side"] == "HOME",
            df_out["home_cover_prob"],
            df_out["away_cover_prob"],
        )

        if "home_spread_odds" in df_out.columns and "away_spread_odds" in df_out.columns:
            pick_odds = np.where(
                df_out["pick_side"] == "HOME",
                pd.to_numeric(df_out["home_spread_odds"], errors="coerce").to_numpy(),
                pd.to_numeric(df_out["away_spread_odds"], errors="coerce").to_numpy(),
            )
            df_out["pick_spread_odds"] = pick_odds

            df_out["pick_breakeven_prob"] = american_to_breakeven_prob(df_out["pick_spread_odds"])
            df_out["pick_prob_edge"] = df_out["pick_cover_prob"] - df_out["pick_breakeven_prob"]

            profit_if_win = american_profit_per_1_staked(df_out["pick_spread_odds"])
            p = df_out["pick_cover_prob"].to_numpy(dtype=float)
            df_out["pick_ev_per_1"] = p * profit_if_win - (1.0 - p) * 1.0

            df_out["pick_fair_odds"] = prob_to_american(df_out["pick_cover_prob"].to_numpy(dtype=float))
        else:
            df_out["pick_spread_odds"] = np.nan
            df_out["pick_breakeven_prob"] = np.nan
            df_out["pick_prob_edge"] = np.nan
            df_out["pick_ev_per_1"] = np.nan
            df_out["pick_fair_odds"] = np.nan

        if "spread_diff" in df_out.columns:
            df_out["spread_diff_old"] = df_out["spread_diff"]
        df_out["spread_diff"] = df_out["edge_points"]
    else:
        df_out["edge_points"] = np.nan
        df_out["edge_z_home"] = np.nan
        df_out["edge_strength"] = np.nan
        df_out["home_cover_prob"] = np.nan
        df_out["away_cover_prob"] = np.nan
        df_out["pick_side"] = np.nan
        df_out["pick_cover_prob"] = np.nan
        df_out["pick_spread_odds"] = np.nan
        df_out["pick_breakeven_prob"] = np.nan
        df_out["pick_prob_edge"] = np.nan
        df_out["pick_ev_per_1"] = np.nan
        df_out["pick_fair_odds"] = np.nan

    # =========================
    # Column ordering / display
    # =========================
    if "pick_ev_per_1" in df_out.columns:
        df_out = df_out.sort_values("pick_ev_per_1", ascending=False)

    edge_cols = [
        "pick_side",
        "pick_cover_prob",
        "pick_spread_odds",
        "pick_breakeven_prob",
        "pick_prob_edge",
        "pick_ev_per_1",
        "pick_fair_odds",
        "edge_strength",
        "edge_z_home",
        "edge_points",
        "home_cover_prob",
        "away_cover_prob",
        "spread_diff",
        "spread_diff_old",
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

    # Default output path: repo_root/predictions/csv/preds_YYYY_M_D_edge.csv
    if out is None or str(out).strip() == "":
        today = _coerce_date(target_date)
        out_path = Path("predictions") / "csv" / f"preds_{today.year}_{today.month}_{today.day}_edge.csv"
    else:
        out_path = Path(out)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_path, index=False)
    print(f"✓ wrote {len(df_out):,} rows → {out_path}")


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
    predict_today_impl(season_year=season_year, out=out)


@cli.command("daily-run")
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
    help="Where to save today's predictions (default: predictions/csv/preds_YYYY_M_D_edge.csv)",
)
def daily_run(season_year: int, out: str | None):
    """
    Update boxscores/data, refresh stats tables, then run predict-today.
    """
    run_repo_script("update_all_data.py")
    run_repo_script("more_stats.py")
    predict_today_impl(season_year=season_year, out=out)
    subprocess.run(["bash", str(REPO_ROOT / "scripts" / "publish_daily.sh")], check=True, cwd=REPO_ROOT)


@cli.command("backfill-season")
@click.option(
    "--season",
    "season_year",
    default=2026,
    show_default=True,
    help="Torvik season key, e.g., 2026 for 2025–26 season",
)
@click.option(
    "--start-date",
    default="2025-11-01",
    show_default=True,
    help="Start date (YYYY-MM-DD) for backfill",
)
@click.option(
    "--end-date",
    default=None,
    show_default=False,
    help="End date (YYYY-MM-DD). Defaults to yesterday.",
)
@click.option(
    "--skip-existing/--no-skip-existing",
    default=True,
    show_default=True,
    help="Skip dates where predictions CSV already exists.",
)
def backfill_season(
    season_year: int,
    start_date: str,
    end_date: str | None,
    skip_existing: bool,
):
    """
    Backfill predictions and JSON outputs for a date range.
    """
    start = _coerce_date(start_date)
    end = _coerce_date(end_date) if end_date else (_dt.date.today() - _dt.timedelta(days=1))
    if end < start:
        raise click.BadParameter("end-date must be on/after start-date")

    day = start
    while day <= end:
        out_path = Path("predictions") / "csv" / f"preds_{day.year}_{day.month}_{day.day}_edge.csv"
        if skip_existing and out_path.exists():
            day += _dt.timedelta(days=1)
            continue
        predict_today_impl(season_year=season_year, out=str(out_path), target_date=day)
        if out_path.exists():
            subprocess.run(
                [
                    sys.executable,
                    str(REPO_ROOT / "scripts" / "csv_to_json.py"),
                    str(out_path),
                    day.strftime("%Y-%m-%d"),
                ],
                check=True,
                cwd=REPO_ROOT,
            )
        day += _dt.timedelta(days=1)

    subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "bart_finals_to_json.py")],
        check=True,
        cwd=REPO_ROOT,
    )



if __name__ == "__main__":
    cli()
