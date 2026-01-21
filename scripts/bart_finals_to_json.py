#!/usr/bin/env python3
from __future__ import annotations

import ast
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd


PREDICTIONS_JSON_DIRS = [
    Path("predictions") / "json",
    Path("site") / "public" / "data",
]


def slugify(text: str) -> str:
    lowered = (text or "").lower()
    slug = re.sub(r"[^a-z0-9]+", "_", lowered)
    return slug.strip("_")


def parse_score_text(score_text: str) -> tuple[int, int] | None:
    match = re.search(r",\s*(\d+)\s*-\s*(\d+)", score_text)
    if not match:
        return None
    home_score, away_score = match.groups()
    return int(away_score), int(home_score)


def parse_boxscore_value(value) -> int | None:
    if value is None:
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def parse_scores(row: pd.Series) -> tuple[int, int] | None:
    boxscore_raw = row.get(50, "")
    if not isinstance(boxscore_raw, str) or not boxscore_raw.strip():
        score_text = row.get(24, "")
        if isinstance(score_text, str):
            parsed = parse_score_text(score_text)
            if parsed:
                return parsed
        return None
    try:
        box = ast.literal_eval(boxscore_raw)
    except (ValueError, SyntaxError):
        score_text = row.get(24, "")
        if isinstance(score_text, str):
            parsed = parse_score_text(score_text)
            if parsed:
                return parsed
        return None

    away_score = parse_boxscore_value(box[18]) if len(box) > 18 else None
    home_score = parse_boxscore_value(box[33]) if len(box) > 33 else None
    if away_score is None or home_score is None:
        score_text = row.get(24, "")
        if isinstance(score_text, str):
            parsed = parse_score_text(score_text)
            if parsed:
                return parsed
        return None
    return away_score, home_score


def extract_prediction_dates() -> list[str]:
    dates: set[str] = set()
    for pred_dir in PREDICTIONS_JSON_DIRS:
        if not pred_dir.exists():
            continue
        for path in pred_dir.glob("predictions_*.json"):
            match = re.search(r"predictions_(\d{4}-\d{2}-\d{2})\.json", path.name)
            if match:
                dates.add(match.group(1))
    return sorted(dates)


def load_prediction_games_by_date(date_str: str) -> set[tuple[str, str]]:
    pred_path = Path("predictions") / "json" / f"predictions_{date_str}.json"
    if not pred_path.exists():
        return set()
    with pred_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    games = payload.get("games", [])
    pairs = set()
    for game in games:
        away = str(game.get("away_team") or "").strip()
        home = str(game.get("home_team") or "").strip()
        if away and home:
            pairs.add((away, home))
    return pairs


def iter_bart_rows(date_strs: Iterable[str]) -> pd.DataFrame:
    bart_path = Path("bart_data") / "2026_super_sked.csv"
    df = pd.read_csv(bart_path, header=None)
    df[1] = pd.to_datetime(df[1], errors="coerce")
    df["date_str"] = df[1].dt.strftime("%Y-%m-%d")
    return df[df["date_str"].isin(set(date_strs))]


def coerce_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def build_payload(date_str: str, bart_df: pd.DataFrame) -> dict:
    predictions_set = load_prediction_games_by_date(date_str)
    games = []
    for _, row in bart_df.iterrows():
        away_team = str(row.get(8, "")).strip()
        home_team = str(row.get(14, "")).strip()
        if not away_team or not home_team:
            continue
        if predictions_set and (away_team, home_team) not in predictions_set:
            continue

        scores = parse_scores(row)
        if not scores:
            continue
        away_score, home_score = scores

        neutral_site = coerce_int(row.get(7))

        game_id = slugify(f"{date_str}_{away_team}_{home_team}")
        games.append(
            {
                "away_team": away_team,
                "home_team": home_team,
                "away_score": away_score,
                "home_score": home_score,
                "neutral_site": neutral_site,
                "game_id": game_id,
            }
        )

    return {
        "date": date_str,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "games": games,
    }


def write_payload(date_str: str, payload: dict) -> None:
    output_dirs = [
        Path("predictions") / "json",
        Path("site") / "public" / "data",
    ]
    for output_dir in output_dirs:
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"final_scores_{date_str}.json"
        with output_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
        print(f"Wrote {output_path}")


def main() -> int:
    date_strs = extract_prediction_dates()
    if not date_strs:
        print("No prediction JSON dates found.")
        return 1

    bart_df = iter_bart_rows(date_strs)
    for date_str in date_strs:
        date_df = bart_df[bart_df["date_str"] == date_str]
        payload = build_payload(date_str, date_df)
        write_payload(date_str, payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
