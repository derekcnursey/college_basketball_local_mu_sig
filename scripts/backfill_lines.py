#!/usr/bin/env python3
import argparse
import datetime as _dt
import re
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from bball.cli import (
    american_profit_per_1_staked,
    american_to_breakeven_prob,
    normal_cdf,
    prob_to_american,
)
from predict_games import attach_hard_rock_lines


def _parse_date(value: str | None) -> _dt.date | None:
    if not value:
        return None
    return _dt.datetime.strptime(value, "%Y-%m-%d").date()


def _date_from_filename(path: Path) -> _dt.date | None:
    match = re.search(r"(\d{4})[_-](\d{1,2})[_-](\d{1,2})", path.name)
    if not match:
        return None
    year, month, day = (int(x) for x in match.groups())
    return _dt.date(year, month, day)


def _recompute_edges(df: pd.DataFrame) -> pd.DataFrame:
    if "home_spread_num" in df.columns and "pred_sigma" in df.columns and "pred_margin" in df.columns:
        mu_arr = pd.to_numeric(df["pred_margin"], errors="coerce").to_numpy(dtype=float)
        sigma_safe = np.clip(pd.to_numeric(df["pred_sigma"], errors="coerce").to_numpy(dtype=float), 1e-6, None)

        home_spread = pd.to_numeric(df["home_spread_num"], errors="coerce").to_numpy(dtype=float)

        edge_points = mu_arr + home_spread
        df["edge_points"] = edge_points

        edge_z_home = edge_points / sigma_safe
        df["edge_z_home"] = edge_z_home
        df["edge_strength"] = np.abs(edge_z_home)

        df["home_cover_prob"] = normal_cdf(edge_z_home)
        df["away_cover_prob"] = 1.0 - df["home_cover_prob"]

        df["pick_side"] = np.where(df["edge_points"] >= 0, "HOME", "AWAY")
        df["pick_cover_prob"] = np.where(
            df["pick_side"] == "HOME",
            df["home_cover_prob"],
            df["away_cover_prob"],
        )

        if "home_spread_odds" in df.columns and "away_spread_odds" in df.columns:
            pick_odds = np.where(
                df["pick_side"] == "HOME",
                pd.to_numeric(df["home_spread_odds"], errors="coerce").to_numpy(),
                pd.to_numeric(df["away_spread_odds"], errors="coerce").to_numpy(),
            )
            df["pick_spread_odds"] = pick_odds

            df["pick_breakeven_prob"] = american_to_breakeven_prob(df["pick_spread_odds"])
            df["pick_prob_edge"] = df["pick_cover_prob"] - df["pick_breakeven_prob"]

            profit_if_win = american_profit_per_1_staked(df["pick_spread_odds"])
            p = df["pick_cover_prob"].to_numpy(dtype=float)
            df["pick_ev_per_1"] = p * profit_if_win - (1.0 - p) * 1.0

            df["pick_fair_odds"] = prob_to_american(df["pick_cover_prob"].to_numpy(dtype=float))
        else:
            df["pick_spread_odds"] = np.nan
            df["pick_breakeven_prob"] = np.nan
            df["pick_prob_edge"] = np.nan
            df["pick_ev_per_1"] = np.nan
            df["pick_fair_odds"] = np.nan

        if "spread_diff" in df.columns:
            df["spread_diff_old"] = df["spread_diff"]
        df["spread_diff"] = df["edge_points"]
    else:
        df["edge_points"] = np.nan
        df["edge_z_home"] = np.nan
        df["edge_strength"] = np.nan
        df["home_cover_prob"] = np.nan
        df["away_cover_prob"] = np.nan
        df["pick_side"] = np.nan
        df["pick_cover_prob"] = np.nan
        df["pick_spread_odds"] = np.nan
        df["pick_breakeven_prob"] = np.nan
        df["pick_prob_edge"] = np.nan
        df["pick_ev_per_1"] = np.nan
        df["pick_fair_odds"] = np.nan

    if "pick_ev_per_1" in df.columns:
        df = df.sort_values("pick_ev_per_1", ascending=False)
    return df


def _update_json(csv_path: Path, date_value: _dt.date) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    script_path = repo_root / "scripts" / "csv_to_json.py"
    subprocess.run(
        [sys.executable, str(script_path), str(csv_path), date_value.strftime("%Y-%m-%d")],
        check=True,
        cwd=repo_root,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill Hard Rock lines into existing prediction CSVs.")
    parser.add_argument("--csv-dir", default="predictions/csv", help="Directory containing prediction CSVs.")
    parser.add_argument("--pattern", default="preds_*_edge.csv", help="Glob pattern for CSVs.")
    parser.add_argument("--start-date", default=None, help="YYYY-MM-DD start date (optional).")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD end date (optional).")
    parser.add_argument("--skip-json", action="store_true", help="Skip regenerating JSON outputs.")
    args = parser.parse_args()

    start_date = _parse_date(args.start_date)
    end_date = _parse_date(args.end_date)

    csv_dir = Path(args.csv_dir)
    files = sorted(csv_dir.glob(args.pattern))
    if not files:
        print(f"No files found in {csv_dir} matching {args.pattern}.")
        return 1

    updated = 0
    for csv_path in files:
        date_value = _date_from_filename(csv_path)
        if not date_value:
            print(f"Skipping (no date in filename): {csv_path}")
            continue
        if start_date and date_value < start_date:
            continue
        if end_date and date_value > end_date:
            continue

        df = pd.read_csv(csv_path)
        df = df.loc[:, ~df.columns.duplicated()]
        drop_cols = [
            "home_spread_num",
            "away_spread_num",
            "home_spread_odds",
            "away_spread_odds",
            "over_num",
            "over_odds",
            "under_num",
            "under_odds",
            "home_winner_odds",
            "away_winner_odds",
            "home_win_odds",
            "away_win_odds",
            "model_home_spread",
            "spread_home",
            "spread_diff",
            "away_winner_diff",
            "home_winner_diff",
            "edge_points",
            "edge_z_home",
            "edge_strength",
            "home_cover_prob",
            "away_cover_prob",
            "pick_side",
            "pick_cover_prob",
            "pick_spread_odds",
            "pick_breakeven_prob",
            "pick_prob_edge",
            "pick_ev_per_1",
            "pick_fair_odds",
            "spread_diff_old",
        ]
        cols_to_drop = [col for col in drop_cols if col in df.columns]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)

        df = attach_hard_rock_lines(df, pred_col="pred_margin", target_date=date_value)
        df = _recompute_edges(df)
        df.to_csv(csv_path, index=False)
        updated += 1
        if not args.skip_json:
            _update_json(csv_path, date_value)

    print(f"Updated {updated} file(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
