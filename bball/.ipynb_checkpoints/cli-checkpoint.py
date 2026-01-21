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
    pass


@cli.command("train")
def train_cmd():
    """Retrain both regressor and classifier on full dataset."""
    df = load_training_dataframe()
    X, y_reg = df.drop(columns=[TARGET_REG, TARGET_CLS]), df[TARGET_REG]
    _, y_cls = None, df[TARGET_CLS]  # same X
    # ──────────────────────────────── NEW ──────────────────────────────── #
    ARTS = Path("artifacts"); ARTS.mkdir(exist_ok=True)

    # 1) save feature order so inference can re-index
    feat_path = ARTS / "feature_order.json"
    json.dump(X.columns.tolist(), feat_path.open("w"))
    print(f"✓ wrote {feat_path}")

    # 2) OPTIONAL: fit & save a StandardScaler (comment out to skip)
    scaler = StandardScaler().fit(X)
    joblib.dump(scaler, ARTS / "scaler.pkl")
    X = pd.DataFrame(scaler.transform(X), columns=X.columns, index=X.index)
    print("✓ fitted & saved StandardScaler")
    # ─────────────────────────── END NEW BLOCK ─────────────────────────── #
    print("Fitting margin-regression model…")
    ckpt_reg = fit_regressor(X, y_reg)
    print(f"✓ saved to {ckpt_reg}")

    print("Fitting win-probability model…")
    ckpt_cls = fit_classifier(X, y_cls)
    print(f"✓ saved to {ckpt_cls}")


@cli.command("predict")
@click.option("--csv-out", default="predictions.csv", help="Where to save results")
def predict_cmd(csv_out):
    """Load latest checkpoints and score today's features (placeholder demo)."""
    # TODO: replace with real today's feature DataFrame
    import pandas as pd

    features_today = pd.read_csv("features_today.csv")  # placeholder

    reg, cls, *_ = load_models(features_today.shape[1])
    preds = predict_margin(features_today, reg)
    probs = predict_home_win_prob(features_today, cls)

    out_df = features_today.copy()
    out_df["pred_margin"] = preds
    out_df["pred_home_win_prob"] = probs
    out_df.to_csv(csv_out, index=False)
    print(f"✓ wrote {csv_out}")



@cli.command("fullrun")
def run_all():
    """Convenience wrapper: train then predict."""
    train_cmd.callback()
    predict_cmd.callback(csv_out="predictions.csv")


@cli.command("tune")
@click.option("--trials", default=30, help="Optuna trials")
def tune_cmd(trials):
    df = load_training_dataframe()
    Xtr, Xv, ytr_reg,yv_reg,ytr_cls,yv_cls  = train_val_split(df, TARGET_REG, TARGET_CLS)
    study_reg, _ = tune(
        Xtr, Xv,
        ytr_reg, yv_reg,
        n_trials=trials,
        tune_classifier=False,
    )
    print("Best regressor params:", study_reg.best_params)

    study_reg.trials_dataframe().to_csv("optuna_reg_trials.csv", index=False)
    print("✓ wrote optuna_*_trials.csv")

@cli.command("train-reg")
def train_reg_cmd():
    """
    Train a FINAL regressor on full dataset using optuna_best_reg.json.
    Saves checkpoints/mlp_regressor.pth
    """
    import json
    from pathlib import Path
    import joblib
    import pandas as pd
    from sklearn.preprocessing import StandardScaler

    df = load_training_dataframe()

    # full X/y (no val split)
    X = df.drop(columns=[TARGET_REG, TARGET_CLS])
    y = df[TARGET_REG]

    # load tuned hyperparams
    best_path = Path("optuna_best_reg.json")
    if not best_path.exists():
        raise FileNotFoundError("optuna_best_reg.json not found. Run: python -m bball.cli tune --trials N")

    best = json.loads(best_path.read_text())

    # --- keep your existing artifacts behavior (feature order + scaler) ---
    ARTS = Path("artifacts"); ARTS.mkdir(exist_ok=True)

    json.dump(X.columns.tolist(), (ARTS / "feature_order.json").open("w"))
    scaler = StandardScaler().fit(X)
    joblib.dump(scaler, ARTS / "scaler.pkl")
    X = pd.DataFrame(scaler.transform(X), columns=X.columns, index=X.index)

    # train final regressor with tuned cfg
    ckpt = fit_regressor(X, y, cfg={
        "hidden": best["hidden"],
        "hidden2": best["hidden2"],
        "dropout": best["dropout"],
        "lr": best["lr"],
        "epochs": best["epochs"],
        "batch_size": best["batch_size"],
        "num_workers": best["num_workers"],
        "ckpt_dir": "checkpoints",
    })

    print("✓ saved final regressor checkpoint:", ckpt)

@cli.command("predict-season")
@click.option("--season", default=2025, show_default=True,
              help="2025 == 2024-25 season")
@click.option("--out",    default="preds_2025.csv",
              help="Where to save predictions")

def predict_season(season: int, out: str):
    """
    Generate model predictions for every game in *season* and
    write them with human-readable columns.
    """
    # 1️⃣  fresh rows (must include INFO_COLS)
    df_all = load_season_data(season)          # keep every column

    # 2️⃣  carve out the parts
    df_info  = df_all[INFO_COLS]               # purely for display
    df_feats = df_all.drop(columns=INFO_COLS)  # model inputs only

    # 3️⃣  line-up columns to training order
    feats_order = json.load(open("artifacts/feature_order.json"))
    df_feats    = df_feats.reindex(columns=feats_order)

    # 4️⃣  apply the saved scaler (if you kept one)
    try:
        scaler   = joblib.load("artifacts/scaler.pkl")
        X_model  = pd.DataFrame(
            scaler.transform(df_feats), columns=feats_order, index=df_feats.index
        )
    except FileNotFoundError:
        X_model = df_feats  # you chose not to scale

    # 5️⃣  load checkpoints (returns feature_order too, in case you need it)
    reg_model, cls_model, _ = load_models(X_model.shape[1])

    # 6️⃣  batched inference
    df_out = df_info.copy()                    # start with the human columns
    df_out["pred_margin"]        = predict_margin(X_model, reg_model)
    df_out["pred_home_win_prob"] = predict_home_win_prob(X_model, cls_model)

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
    default="preds_today.csv",
    show_default=True,
    help="Where to save today's predictions",
)
def predict_today(season_year: int, out: str):
    """
    Generate model predictions for *today's* games only.
    """
    import json, joblib
    import pandas as pd
    from pathlib import Path

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

    # 3️⃣ Load checkpoints
    reg_model, cls_model, _ = load_models(X_model.shape[1])

    # 4️⃣ Predictions
    df_out = info_df.copy()
    df_out["pred_margin"] = predict_margin(X_model, reg_model)
    df_out["pred_home_win_prob"] = predict_home_win_prob(X_model, cls_model)

    # 5️⃣ 🔥 Attach Hard Rock lines + edge metrics
    df_out = attach_hard_rock_lines(df_out, pred_col="pred_margin")
    # 🔢 Sort by absolute spread_diff (biggest edges first)
    if "spread_diff" in df_out.columns:
        df_out = df_out.sort_values(
            "spread_diff", key=lambda s: s.abs(), ascending=False
        )
    preferred_prefix = [
        "date",
        "away_team_name",
        "home_team_name",
        "neutral_site",
        "spread_diff",
    ]
    prefix_cols = [c for c in preferred_prefix if c in df_out.columns]
    other_cols  = [c for c in df_out.columns if c not in prefix_cols]
    df_out = df_out[prefix_cols + other_cols]

    Path(out).parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out, index=False)
    print(f"✓ wrote {len(df_out):,} rows → {out}")



if __name__ == "__main__":
    cli()
