#!/usr/bin/env python3
"""Backfill betting lines from hoops-edge S3 lakehouse into prediction CSVs.

Reads fct_lines from S3 (DK > ESPN BET > Bovada provider preference),
matches to this repo's predictions by team name + date, recomputes edges,
and regenerates site JSONs.

Usage:
  python scripts/backfill_s3_lines.py
  python scripts/backfill_s3_lines.py --start-date 2026-03-01
  python scripts/backfill_s3_lines.py --dry-run
"""
import argparse
import datetime as _dt
import re
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# --- S3 reading (inline to avoid import issues) ---

import io
import boto3
import pyarrow.parquet as pq

S3_BUCKET = "hoops-edge"
S3_REGION = "us-east-1"
SILVER_PREFIX = "silver"
TABLE_FCT_LINES = "fct_lines"

PROVIDER_RANK = {"Draft Kings": 0, "ESPN BET": 1, "Bovada": 2}

# --- Team name mapping: local name -> S3 name ---

LOCAL_TO_S3 = {
    "Alabama St.": "Alabama State",
    "Albany": "UAlbany",
    "Alcorn St.": "Alcorn State",
    "American": "American University",
    "Appalachian St.": "App State",
    "Arizona St.": "Arizona State",
    "Arkansas Pine Bluff": "Arkansas-Pine Bluff",
    "Arkansas St.": "Arkansas State",
    "Ball St.": "Ball State",
    "Bethune Cookman": "Bethune-Cookman",
    "Boise St.": "Boise State",
    "Cal Baptist": "California Baptist",
    "Cal St. Bakersfield": "Cal State Bakersfield",
    "Cal St. Fullerton": "Cal State Fullerton",
    "Cal St. Northridge": "Cal State Northridge",
    "Chicago St.": "Chicago State",
    "Cleveland St.": "Cleveland State",
    "Colorado St.": "Colorado State",
    "Connecticut": "UConn",
    "Coppin St.": "Coppin State",
    "Delaware St.": "Delaware State",
    "East Tennessee St.": "East Tennessee State",
    "FIU": "Florida International",
    "Florida St.": "Florida State",
    "Fresno St.": "Fresno State",
    "Gardner Webb": "Gardner-Webb",
    "Georgia St.": "Georgia State",
    "Grambling St.": "Grambling",
    "Hawaii": "Hawai'i",
    "IU Indy": "IU Indianapolis",
    "Idaho St.": "Idaho State",
    "Illinois Chicago": "UIC",
    "Illinois St.": "Illinois State",
    "Indiana St.": "Indiana State",
    "Iowa St.": "Iowa State",
    "Jackson St.": "Jackson State",
    "Jacksonville St.": "Jacksonville State",
    "Kansas St.": "Kansas State",
    "Kennesaw St.": "Kennesaw State",
    "Kent St.": "Kent State",
    "LIU": "Long Island University",
    "Long Beach St.": "Long Beach State",
    "Louisiana Monroe": "UL Monroe",
    "Loyola MD": "Loyola Maryland",
    "McNeese St.": "McNeese",
    "Miami FL": "Miami",
    "Miami OH": "Miami (OH)",
    "Michigan St.": "Michigan State",
    "Mississippi": "Ole Miss",
    "Mississippi St.": "Mississippi State",
    "Mississippi Valley St.": "Mississippi Valley State",
    "Missouri St.": "Missouri State",
    "Montana St.": "Montana State",
    "Morehead St.": "Morehead State",
    "Morgan St.": "Morgan State",
    "Murray St.": "Murray State",
    "N.C. State": "NC State",
    "Nebraska Omaha": "Omaha",
    "New Mexico St.": "New Mexico State",
    "Nicholls St.": "Nicholls",
    "Norfolk St.": "Norfolk State",
    "North Dakota St.": "North Dakota State",
    "Northwestern St.": "Northwestern State",
    "Ohio St.": "Ohio State",
    "Oklahoma St.": "Oklahoma State",
    "Oregon St.": "Oregon State",
    "Penn": "Pennsylvania",
    "Penn St.": "Penn State",
    "Portland St.": "Portland State",
    "Queens": "Queens University",
    "Sacramento St.": "Sacramento State",
    "Saint Francis": "St. Francis (PA)",
    "Sam Houston St.": "Sam Houston",
    "San Diego St.": "San Diego State",
    "San Jose St.": "San Jos\u00e9 State",
    "Seattle": "Seattle U",
    "South Carolina St.": "South Carolina State",
    "South Dakota St.": "South Dakota State",
    "Southeast Missouri St.": "Southeast Missouri State",
    "Southeastern Louisiana": "SE Louisiana",
    "St. Thomas": "St. Thomas-Minnesota",
    "Tarleton St.": "Tarleton State",
    "Tennessee Martin": "UT Martin",
    "Tennessee St.": "Tennessee State",
    "Texas A&M Corpus Chris": "Texas A&M-Corpus Christi",
    "Texas St.": "Texas State",
    "UMKC": "Kansas City",
    "USC Upstate": "South Carolina Upstate",
    "Utah St.": "Utah State",
    "Washington St.": "Washington State",
    "Weber St.": "Weber State",
    "Wichita St.": "Wichita State",
    "Wright St.": "Wright State",
    "Youngstown St.": "Youngstown State",
}

# Reverse mapping for S3 -> local
S3_TO_LOCAL = {v: k for k, v in LOCAL_TO_S3.items()}


def to_s3_name(local_name: str) -> str:
    return LOCAL_TO_S3.get(local_name, local_name)


# --- Edge computation helpers (from bball.cli) ---

def normal_cdf(x):
    """Standard normal CDF using the error function."""
    from math import erf, sqrt
    if isinstance(x, (int, float)):
        return 0.5 * (1.0 + erf(x / sqrt(2.0)))
    return 0.5 * (1.0 + np.vectorize(erf)(np.asarray(x, dtype=float) / np.sqrt(2.0)))


def american_to_breakeven_prob(odds):
    odds = np.asarray(odds, dtype=float)
    result = np.where(
        odds < 0,
        -odds / (-odds + 100.0),
        100.0 / (odds + 100.0),
    )
    return result


def american_profit_per_1_staked(odds):
    odds = np.asarray(odds, dtype=float)
    return np.where(odds < 0, 100.0 / -odds, odds / 100.0)


def prob_to_american(prob):
    prob = np.asarray(prob, dtype=float)
    result = np.where(
        prob >= 0.5,
        -prob / (1.0 - prob) * 100.0,
        (1.0 - prob) / prob * 100.0,
    )
    return np.round(result).astype(float)


# --- S3 reading ---

def _s3_client():
    return boto3.client("s3", region_name=S3_REGION)


def _read_s3_lines(season: int) -> pd.DataFrame:
    """Read fct_lines from S3 for a given season."""
    prefix = f"{SILVER_PREFIX}/{TABLE_FCT_LINES}/season={season}/"
    client = _s3_client()

    # Check for asof= sub-partitions first
    resp = client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, Delimiter="/")
    sub_prefixes = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
    asof_prefixes = sorted([p for p in sub_prefixes if "asof=" in p], reverse=True)

    scan_prefix = asof_prefixes[0] if asof_prefixes else prefix

    # List parquet files
    keys = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=scan_prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])

    if not keys:
        return pd.DataFrame()

    # Read and concat
    dfs = []
    for key in keys:
        resp = client.get_object(Bucket=S3_BUCKET, Key=key)
        data = resp["Body"].read()
        tbl = pq.read_table(io.BytesIO(data))
        dfs.append(tbl.to_pandas())

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def _dedup_lines(lines_df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate lines: prefer complete data, then DK > ESPN BET > Bovada.
    Fix spread sign via majority vote."""
    lines_df = lines_df.copy()
    lines_df["spread"] = pd.to_numeric(lines_df["spread"], errors="coerce")
    lines_df["homeMoneyline"] = pd.to_numeric(lines_df["homeMoneyline"], errors="coerce")

    # Spread sign fix: majority vote
    has_spread = lines_df["spread"].notna() & (lines_df["spread"] != 0)
    spread_sign = np.sign(lines_df.loc[has_spread, "spread"])
    majority_sign = (
        spread_sign.groupby(lines_df.loc[has_spread, "gameId"])
        .sum()
        .rename("_majority_sign")
    )

    dedup = (
        lines_df
        .assign(
            _has_spread=lines_df["spread"].notna().astype(int),
            _has_total=lines_df["overUnder"].notna().astype(int),
            _prov_rank=lines_df["provider"].map(PROVIDER_RANK).fillna(99),
        )
        .sort_values(
            ["_has_spread", "_has_total", "_prov_rank"],
            ascending=[False, False, True],
        )
        .drop_duplicates(subset=["gameId"], keep="first")
        .drop(columns=["_has_spread", "_has_total", "_prov_rank"])
        .copy()
    )

    # Apply majority sign flip
    dedup = dedup.merge(majority_sign, on="gameId", how="left")
    _sp = dedup["spread"]
    _maj = dedup["_majority_sign"]
    mask = (
        _sp.notna() & _maj.notna() & (_maj != 0)
        & (abs(_sp) >= 3)
        & (np.sign(_sp) != np.sign(_maj))
    )
    dedup.loc[mask, "spread"] = -_sp[mask]

    # Moneyline cross-check for single-provider games
    _sp2 = dedup["spread"]
    _ml = dedup["homeMoneyline"]
    mask_ml = (
        _sp2.notna() & _ml.notna()
        & (~mask)
        & dedup["_majority_sign"].isna()
        & (((_sp2 > 3) & (_ml < -150)) | ((_sp2 < -3) & (_ml > 150)))
    )
    dedup.loc[mask_ml, "spread"] = -_sp2[mask_ml]
    dedup = dedup.drop(columns=["_majority_sign"])

    # Extract date from startDate in Eastern Time (predictions use ET dates)
    ts = pd.to_datetime(dedup["startDate"])
    if ts.dt.tz is not None:
        dedup["game_date"] = ts.dt.tz_convert("America/New_York").dt.strftime("%Y-%m-%d")
    else:
        dedup["game_date"] = ts.dt.tz_localize("UTC").dt.tz_convert("America/New_York").dt.strftime("%Y-%m-%d")

    return dedup


# --- Edge recomputation ---

def _recompute_edges(df: pd.DataFrame) -> pd.DataFrame:
    """Recompute edge metrics from pred_margin + home_spread_num."""
    if "home_spread_num" not in df.columns or "pred_sigma" not in df.columns or "pred_margin" not in df.columns:
        return df

    mu_arr = pd.to_numeric(df["pred_margin"], errors="coerce").to_numpy(dtype=float)
    sigma_safe = np.clip(pd.to_numeric(df["pred_sigma"], errors="coerce").to_numpy(dtype=float), 1e-6, None)
    home_spread = pd.to_numeric(df["home_spread_num"], errors="coerce").to_numpy(dtype=float)

    edge_points = mu_arr + home_spread
    df["edge_points"] = edge_points
    df["edge_z_home"] = edge_points / sigma_safe
    df["edge_strength"] = np.abs(df["edge_z_home"])
    df["home_cover_prob"] = normal_cdf(df["edge_z_home"])
    df["away_cover_prob"] = 1.0 - df["home_cover_prob"]
    df["pick_side"] = np.where(edge_points >= 0, "HOME", "AWAY")
    df["pick_cover_prob"] = np.where(
        df["pick_side"] == "HOME",
        df["home_cover_prob"],
        df["away_cover_prob"],
    )

    # Model home spread (negative = home favored, for display)
    df["model_home_spread"] = -mu_arr

    # Use -110 standard odds
    pick_odds = -110.0
    if "home_spread_odds" in df.columns and "away_spread_odds" in df.columns:
        pick_odds_arr = np.where(
            df["pick_side"] == "HOME",
            pd.to_numeric(df["home_spread_odds"], errors="coerce").to_numpy(),
            pd.to_numeric(df["away_spread_odds"], errors="coerce").to_numpy(),
        )
        # Fall back to -110 where odds are missing
        pick_odds_arr = np.where(np.isnan(pick_odds_arr), -110.0, pick_odds_arr)
        df["pick_spread_odds"] = pick_odds_arr
    else:
        df["pick_spread_odds"] = -110.0
        pick_odds_arr = np.full(len(df), -110.0)

    df["pick_breakeven_prob"] = american_to_breakeven_prob(pick_odds_arr)
    df["pick_prob_edge"] = df["pick_cover_prob"] - df["pick_breakeven_prob"]

    profit_if_win = american_profit_per_1_staked(pick_odds_arr)
    p = df["pick_cover_prob"].to_numpy(dtype=float)
    df["pick_ev_per_1"] = p * profit_if_win - (1.0 - p)
    df["pick_fair_odds"] = prob_to_american(p)

    df["spread_diff"] = edge_points

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


def _date_from_filename(path: Path) -> _dt.date | None:
    match = re.search(r"(\d{4,5})[_-](\d{1,2})[_-](\d{1,2})", path.name)
    if not match:
        return None
    year, month, day = (int(x) for x in match.groups())
    # Fix typo filenames like preds_20026_... -> 2026
    if year > 2100:
        year = year - 18000  # 20026 -> 2026
    return _dt.date(year, month, day)


def _get_season(d: _dt.date) -> int:
    return d.year + 1 if d.month >= 11 else d.year


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill S3 lines into prediction CSVs")
    parser.add_argument("--csv-dir", default="predictions/csv", help="Directory containing prediction CSVs")
    parser.add_argument("--start-date", default=None, help="YYYY-MM-DD start date")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD end date")
    parser.add_argument("--dry-run", action="store_true", help="Show what would change")
    parser.add_argument("--skip-json", action="store_true", help="Skip JSON regeneration")
    args = parser.parse_args()

    start_date = _dt.datetime.strptime(args.start_date, "%Y-%m-%d").date() if args.start_date else None
    end_date = _dt.datetime.strptime(args.end_date, "%Y-%m-%d").date() if args.end_date else None

    csv_dir = Path(args.csv_dir)
    files = sorted(csv_dir.glob("preds_*_edge.csv"))
    if not files:
        print(f"No CSV files found in {csv_dir}")
        return 1

    # Group files by season
    files_by_season: dict[int, list[tuple[Path, _dt.date]]] = {}
    for f in files:
        d = _date_from_filename(f)
        if not d:
            continue
        if start_date and d < start_date:
            continue
        if end_date and d > end_date:
            continue
        season = _get_season(d)
        files_by_season.setdefault(season, []).append((f, d))

    total = sum(len(v) for v in files_by_season.values())
    print(f"Processing {total} files across {len(files_by_season)} season(s)...")

    updated = 0
    gained_lines = 0
    lost_lines = 0

    for season in sorted(files_by_season):
        print(f"\n  Season {season}: loading lines from S3...")
        raw_lines = _read_s3_lines(season)
        if raw_lines.empty:
            print(f"  Season {season}: no lines found, skipping")
            continue

        lines = _dedup_lines(raw_lines)
        print(f"  Got {len(lines)} deduplicated lines across {lines['game_date'].nunique()} dates")

        # Build lookup: (s3_home, s3_away, date) -> line row
        lines_lookup: dict[tuple[str, str, str], pd.Series] = {}
        for _, row in lines.iterrows():
            key = (str(row.get("homeTeam", "")), str(row.get("awayTeam", "")), str(row.get("game_date", "")))
            lines_lookup[key] = row

        for csv_path, date_value in files_by_season[season]:
            date_str = date_value.strftime("%Y-%m-%d")
            df = pd.read_csv(csv_path)
            df = df.loc[:, ~df.columns.duplicated()]

            old_has_line = df["home_spread_num"].notna().sum() if "home_spread_num" in df.columns else 0

            # Drop old line/edge columns
            drop_cols = [
                "home_spread_num", "away_spread_num",
                "home_spread_odds", "away_spread_odds",
                "over_total_num", "over_total_odds",
                "under_total_num", "under_total_odds",
                "home_winner_odds", "away_winner_odds",
                "home_win_odds", "away_win_odds",
                "model_home_spread", "spread_home",
                "spread_diff", "spread_diff_old",
                "away_winner_diff", "home_winner_diff",
                "edge_points", "edge_z_home", "edge_strength",
                "home_cover_prob", "away_cover_prob",
                "pick_side", "pick_cover_prob",
                "pick_spread_odds", "pick_breakeven_prob",
                "pick_prob_edge", "pick_ev_per_1", "pick_fair_odds",
            ]
            cols_to_drop = [c for c in drop_cols if c in df.columns]
            if cols_to_drop:
                df = df.drop(columns=cols_to_drop)

            # Match lines by team name
            spreads = []
            spread_odds_home = []
            spread_odds_away = []
            for _, row in df.iterrows():
                away_local = row.get("away_team_name", "")
                home_local = row.get("home_team_name", "")
                away_s3 = to_s3_name(away_local)
                home_s3 = to_s3_name(home_local)

                line = lines_lookup.get((home_s3, away_s3, date_str))
                if line is not None and pd.notna(line.get("spread")):
                    spreads.append(float(line["spread"]))
                    spread_odds_home.append(-110.0)
                    spread_odds_away.append(-110.0)
                else:
                    spreads.append(np.nan)
                    spread_odds_home.append(np.nan)
                    spread_odds_away.append(np.nan)

            df["home_spread_num"] = spreads
            df["away_spread_num"] = [-s if pd.notna(s) else np.nan for s in spreads]
            df["home_spread_odds"] = spread_odds_home
            df["away_spread_odds"] = spread_odds_away

            df = _recompute_edges(df)

            new_has_line = df["home_spread_num"].notna().sum()
            diff = new_has_line - old_has_line

            label = f"  {csv_path.name}: {len(df)} games, {new_has_line} with lines"
            if diff > 0:
                label += f" (+{diff} gained)"
                gained_lines += diff
            elif diff < 0:
                label += f" ({diff} lost)"
                lost_lines += abs(diff)

            if args.dry_run:
                print(f"  [DRY RUN] {label}")
            else:
                df.to_csv(csv_path, index=False)
                print(label)
                if not args.skip_json:
                    _update_json(csv_path, date_value)
                updated += 1

    print(f"\n{'[DRY RUN] ' if args.dry_run else ''}Done. Updated {updated} files.")
    print(f"  Lines gained: +{gained_lines}, lost: -{lost_lines}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
